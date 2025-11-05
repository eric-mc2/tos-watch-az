from azure import durable_functions as df
from datetime import timedelta
import logging
import json
from src.log_utils import setup_logger
from dataclasses import dataclass, asdict, fields
from src.rate_limiter import TRY_ACQUIRE
from src.circuit_breaker import TRIP, GET_STATUS, RESET
from typing import Literal

logger = setup_logger(__name__, logging.INFO)

WorkflowType = Literal["summarizer", "scraper", "meta"]

@dataclass
class OrchData:
    company: str
    task_id: str
    workflow_type: WorkflowType

    def to_dict(self):
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data):
        arg_names = [f.name for f in fields(cls)]
        return cls(**{k:v for k,v in data.items() if k in arg_names})

@dataclass
class WorkflowConfig:
    rate_limit_rpm: int
    throttle_delay: float
    processor_name: str
    max_retries: int

    def to_dict(self):
        return asdict(self)

WORKFLOW_CONFIGS = {
    "summarizer": WorkflowConfig(50, 2, "summarizer_processor", 3).to_dict(),
    "scraper": WorkflowConfig(5, 2, "scraper_processor", 3).to_dict(),
    "meta": WorkflowConfig(5, 20, "meta_processor", 3).to_dict()
}


def orchestrator_logic(context: df.DurableOrchestrationContext):
    """Generic Orchestrator that enforces rate limiting using the durable entity."""
    input_data = context.get_input()
    workflow_type = input_data.get("workflow_type")
    task_id = input_data.get("task_id")

    # Tack on config info
    input_data |= WORKFLOW_CONFIGS[workflow_type]
           
    rate_limiter_id = df.EntityId("rate_limiter", workflow_type)
    circuit_breaker_id = df.EntityId("circuit_breaker", workflow_type)    

    # Check circuit breaker first
    allowed = yield context.call_entity(circuit_breaker_id, GET_STATUS)
    if not allowed:
        logger.warning(f"Circuit breaker open for {workflow_type}, aborting: {task_id}")
        raise RuntimeError(f"Circuit breaker open.")

    # Wait for rate limit token
    while True:
        # Pass config and time to entity
        entity_input = input_data | {"last_success_time": context.current_utc_datetime.isoformat()}
        allowed = yield context.call_entity(rate_limiter_id, TRY_ACQUIRE, entity_input)
        if allowed:
            break
        # Wait before retrying
        delay = input_data.get("throttle_delay", 5)
        retry_time = context.current_utc_datetime + timedelta(seconds=delay)
        if not context.is_replaying:
            # This logs every poll. ==> A replay is for a failure, not a wake up.
            logger.debug(f"Throttling {workflow_type} retry at {retry_time} : {task_id}")
        yield context.create_timer(retry_time)
    
    logger.debug("Orchestrator passed rate limiter and calling activity: %s", input_data["processor_name"])
    
    try:
        result = yield context.call_activity(input_data["processor_name"], input_data)
        logger.debug(f"Successfully processed {task_id}")
        return result # must signal runtime with explicit return
        
    except Exception as e:
        logger.error(f"Processor {workflow_type} failed {task_id}")
        yield context.call_entity(circuit_breaker_id, TRIP, str(e))
        raise