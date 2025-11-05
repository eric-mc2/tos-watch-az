from azure import durable_functions as df
from datetime import timedelta
import logging
import json
from src.log_utils import setup_logger
from dataclasses import dataclass, asdict
from src.rate_limiter import TRY_ACQUIRE
from src.circuit_breaker import TRIP, GET_STATUS, RESET

logger = setup_logger(__name__, logging.DEBUG)


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
    "meta": WorkflowConfig(20, 20, "meta_processor", 3).to_dict()
}


def orchestrator_logic(context: df.DurableOrchestrationContext):
    """Generic Orchestrator that enforces rate limiting using the durable entity."""
    input_data = context.get_input()
    workflow_type = input_data.get("workflow_type")
    task_id = input_data.get("task_id")
    
    logger.debug(f"Orchestrator started {workflow_type} replay is {context.is_replaying}: {task_id}")
        
    rate_limiter_id = df.EntityId("generic_rate_limiter_entity", workflow_type)
    circuit_breaker_id = df.EntityId("circuit_breaker_entity_func", workflow_type)    

    # Check circuit breaker first
    logger.debug(f"Checking circuit breaker {workflow_type} workflow for {task_id}")
    allowed = yield context.call_entity(circuit_breaker_id, GET_STATUS)
    if not allowed:
        logger.warning(f"Circuit breaker open for {workflow_type}, aborting: {task_id}")
        raise RuntimeError(f"Circuit breaker open.")

    # Wait for rate limit token
    while True:
        # Pass config and time to entity
        entity_input = input_data | {"current_time": context.current_utc_datetime.isoformat()}
        if not context.is_replaying:
            logger.debug(f"Checking rate limiter {workflow_type} workflow for {task_id}")
        allowed = yield context.call_entity(rate_limiter_id, TRY_ACQUIRE, entity_input)
        if allowed:
            break
        # Wait before retrying
        delay = input_data.get("throttle_delay", 5)
        retry_time = context.current_utc_datetime + timedelta(seconds=delay)
        if not context.is_replaying:
            logger.warning(f"Throttling {workflow_type} retry at {retry_time} : {task_id}")
        if task_id is None:
            raise RuntimeError("task_id is None. config is %s. data is %s", json.dumps(input_data))
        yield context.create_timer(retry_time)
    
    logger.debug("Orchestrator passed rate limiter and calling activity: %s", input_data["processor_name"])
    
    try:
        result = yield context.call_activity(input_data["processor_name"], input_data)
        logger.info(f"Successfully processed {task_id}")
        yield result
        
    except Exception as e:
        logger.error(f"Processor {workflow_type} failed {task_id}")
        yield context.call_entity(circuit_breaker_id, TRIP, str(e))
        raise