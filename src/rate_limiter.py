from azure import durable_functions as df
from datetime import datetime, timedelta, timezone
import logging
from src.log_utils import setup_logger
import json

logger = setup_logger(__name__, logging.INFO)

def rate_limiter_entity(context: df.DurableEntityContext, config: dict):
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    logger.debug(f"Calling rate limiter with entity_key: {context.entity_key}")
    
    # Get config and current_time from input (passed by orchestrator)
    input_data = context.get_input()
    
    # Handle operations that don't need input_data
    operation = context.operation_name
    
    if operation == "get_status":
        current_state = context.get_state(lambda: {
            "tokens": 0,
            "last_refill": None,
        })
        context.set_result(current_state)
        context.set_state(current_state)
        return
    
    # For operations that need config
    if not input_data:
        logger.error(f"No input data provided for operation {operation} on entity {context.entity_key}")
        context.set_result(False)
        return
    
    workflow_config = input_data.get("config", {})
    current_time_str = input_data.get("current_time")
    
    if not workflow_config:
        logger.error(f"No config provided in input for entity {context.entity_key}")
        context.set_result(False)
        return
    
    rate_limit_rpm = workflow_config["rate_limit_rpm"]
    
    current_state = context.get_state(lambda: {
        "tokens": rate_limit_rpm,
        "last_refill": None,
    })
    
    current_time = datetime.fromisoformat(current_time_str) if current_time_str else datetime.now(timezone.utc)
    
    if current_state["last_refill"] is None:
        current_state["last_refill"] = current_time.isoformat()
        current_state["tokens"] = rate_limit_rpm
    else:
        last_refill = datetime.fromisoformat(current_state["last_refill"])
        
        # Calculate time elapsed since last refill
        time_elapsed = (current_time - last_refill).total_seconds()
        
        # Refill tokens based on elapsed time
        if time_elapsed >= 60:
            current_state["tokens"] = rate_limit_rpm
            current_state["last_refill"] = current_time.isoformat()
    
    if operation == "try_consume":
        if current_state["tokens"] > 0:
            current_state["tokens"] -= 1
            context.set_result(True)
        else:
            context.set_result(False)
    else:
        # Unknown operation - return False to be safe
        logger.warning(f"Unknown operation '{operation}' for rate limiter entity")
        context.set_result(False)
    
    context.set_state(current_state)
    logger.debug(f"Rate limiter finished with result: {context._result} and state: {context.get_state()}")
    
def orchestrator_logic(context: df.DurableOrchestrationContext, config: dict):
    """Generic Orchestrator that enforces rate limiting using the durable entity."""
    input_data = context.get_input()
    workflow_type = input_data.get("workflow_type")
    task_id = input_data.get("task_id")    
    if not context.is_replaying:
        # For debugging:
        context.set_custom_status({"workflow_type": workflow_type, "task_id": task_id})

    logger.debug(f"Orchestrator started {workflow_type} replay is {context.is_replaying}: {task_id}")
    if workflow_type not in config:
        raise ValueError(f"Unknown workflow type: {workflow_type}")
    
    workflow_config = config[workflow_type]
    rate_limiter_id = df.EntityId("generic_rate_limiter_entity", workflow_type)
    circuit_breaker_id = df.EntityId("circuit_breaker_entity_func", workflow_type)    

    # Check circuit breaker first
    logger.debug(f"Checking circuit breaker {workflow_type} workflow for {task_id}")
    circuit_status = yield context.call_entity(circuit_breaker_id, "get_status")
    if circuit_status.get("is_open", False):
        logger.warning(f"Circuit breaker open for {workflow_type}, aborting: {task_id}")
        raise Exception(f"Circuit breaker open.")

    # Wait for rate limit token
    while True:
        # Pass config and time to entity
        entity_input = {
            "config": workflow_config,
            "current_time": context.current_utc_datetime.isoformat()
        }
        if not context.is_replaying:
            logger.debug(f"Checking rate limiter {workflow_type} workflow for {task_id}")
        allowed = yield context.call_entity(rate_limiter_id, "try_consume", entity_input)
        if allowed:
            break
        # Wait before retrying
        delay = workflow_config.get("delay", 5)
        retry_time = context.current_utc_datetime + timedelta(seconds=delay)
        if not context.is_replaying:
            logger.warning(f"Throttling {workflow_type} retry at {retry_time} : {task_id}")
        if task_id is None:
            raise RuntimeError("task_id is None. config is %s. data is %s", json.dumps(workflow_config), json.dumps(input_data))
        yield context.create_timer(retry_time)
    
    logger.debug("Orchestrator passed rate limiter and calling activity: %s", workflow_config["activity_name"])
    
    try:
        result = yield context.call_activity(workflow_config["activity_name"], input_data)
        logger.info(f"Successfully processed {task_id}")
        return result
        
    except Exception as e:
        logger.error(f"Processor {workflow_type} failed {task_id}")
        yield context.call_entity(circuit_breaker_id, "trip", str(e))
        raise