from azure import durable_functions as df
from datetime import timedelta
import logging
from typing import Literal
from dataclasses import dataclass, asdict, fields
import json
from src.rate_limiter import TRY_ACQUIRE
from src.log_utils import setup_logger
from src.circuit_breaker import TRIP, GET_STATUS
from src.app_utils import AppError

logger = setup_logger(__name__, logging.INFO)

@dataclass
class OrchData:
    task_id: str
    workflow_type: Literal["summarizer", "scraper", "meta"]
    company: str = ""
    policy: str = ""
    timestamp: str = ""

    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        arg_names = [f.name for f in fields(cls)]
        return cls(**{k:v for k,v in data.items() if k in arg_names})

@dataclass
class WorkflowConfig:
    rate_limit_rpm: int
    rate_limit_period: int  # seconds
    throttle_delay: float   # seconds
    processor_name: str
    max_attempts: int
    retry_delay: float      # seconds


    def to_dict(self):
        return asdict(self)


WORKFLOW_CONFIGS = {
    "summarizer": WorkflowConfig(50, 60, 20, "summarizer_processor", 3, 10),
    "scraper": WorkflowConfig(10, 60, 20, "scraper_processor", 3, 10),
    "meta": WorkflowConfig(10, 60, 20, "meta_processor", 3, 10)
}

CIRCUIT_DELAY = 60 * 5  # seconds


def orchestrator_logic(context: df.DurableOrchestrationContext, configs: dict[str, WorkflowConfig]=WORKFLOW_CONFIGS):
    """Generic Orchestrator that enforces rate limiting using the durable entity."""
    input_data = context.get_input()
    workflow_type = input_data.get("workflow_type")
    task_id = input_data.get("task_id")
    
    # Tack on config info (for entities)
    input_data |= configs[workflow_type].to_dict()
    processor_name = configs[workflow_type].processor_name
    circuit_breaker_id = df.EntityId("circuit_breaker", workflow_type)    

    # First circuit check fails fast.
    allowed = yield from _check_circuit_logic(context, configs[workflow_type])
    if not allowed:
        # Already signaled continue as new.
        return None
        
    try:
        result = yield from _retry_logic(context, configs[workflow_type])
        logger.debug(f"Successfully processed {task_id}")
        return result # must signal runtime with explicit return
        
    except Exception as e:
        logger.error(f"Processor {workflow_type} failed {task_id} with error:\n{e}")
        yield context.call_entity(circuit_breaker_id, TRIP, str(e))
        raise


def _check_circuit_logic(context: df.DurableOrchestrationContext, config: WorkflowConfig):
    input_data = context.get_input()
    task_id = input_data.get("task_id")
    workflow_type = input_data.get("workflow_type")

    circuit_breaker_id = df.EntityId("circuit_breaker", workflow_type) 
    
    allowed = yield context.call_entity(circuit_breaker_id, GET_STATUS)
    if not allowed:
        logger.warning(f"Circuit breaker open for {workflow_type}, sleeping: {task_id}")
        retry_time = context.current_utc_datetime + timedelta(seconds = CIRCUIT_DELAY)
        yield context.create_timer(retry_time)
        context.continue_as_new(input_data)
    return allowed

    
def _rate_limit_logic(context: df.DurableOrchestrationContext, config: WorkflowConfig):
    input_data = context.get_input()
    task_id = input_data.get("task_id")
    workflow_type = input_data.get("workflow_type")
    throttle_delay = config.throttle_delay
    rate_period = config.rate_limit_period
    
    rate_limiter_id = df.EntityId("rate_limiter", workflow_type)

    while True:
        # Pass config and time to entity
        entity_input = input_data | {"last_success_time": context.current_utc_datetime.isoformat()}
        allowed = yield context.call_entity(rate_limiter_id, TRY_ACQUIRE, entity_input)
        if allowed:
            return True
        # Wait before retrying
        retry_time = context.current_utc_datetime + timedelta(seconds = throttle_delay)
        if not context.is_replaying:
            # This logs every poll. ==> A replay is for a failure, not a wake up.
            logger.debug(f"Throttling {workflow_type} retry at {retry_time} : {task_id}")
        yield context.create_timer(retry_time)

    
def _retry_logic(context: df.DurableOrchestrationContext, config: WorkflowConfig):
    input_data = context.get_input()
    task_id = input_data.get("task_id")
    workflow_type = input_data.get("workflow_type")
    retry_delay = config.retry_delay
    max_attempts = config.max_attempts
    processor_name = config.processor_name

    result = None
    managed_error = None
    for attempt_count in range(1, max_attempts + 1):
        # Events pool inside rate limit loop.
        allowed = yield from _rate_limit_logic(context, config)
        if not allowed:
            raise RuntimeError("Should not get here.")
        
        # Check circuit again (in case tripped while awaiting rate).
        allowed = yield from _check_circuit_logic(context, config)
        if not allowed:
            # Already signaled continue as new.
            return None  # exit cleanly.
        
        result = yield context.call_activity(processor_name, input_data)
        
        try:
            if isinstance(result, dict) and "error_type" in result:
                managed_error = AppError(**result)
            else:
                managed_error = None  # Reset error if result is not an error
        except Exception as ae:
            managed_error = None  # it wasn't actually an error

        if managed_error is None:
            break  # activity succeeded. exit loop.

        if attempt_count == max_attempts:
            break  # activity failed but we dont want to retry.

        logger.warning(f"Processor {workflow_type} failed {task_id}. " \
                        f"Retrying ({attempt_count}/{max_attempts}) from error: {managed_error}")
        
        retry_time = context.current_utc_datetime + timedelta(seconds = retry_delay)
        yield context.create_timer(retry_time)
    
    if managed_error is not None:
        # last attempt failed
        error_msg = managed_error.to_dict()
        error_msg["workflow_type"] = workflow_type
        error_msg["task_id"] = task_id
        raise Exception(json.dumps(error_msg, indent=2))
    
    return result