import azure.functions as func
from azure import durable_functions as df
from datetime import datetime, timedelta, timezone
import json
import logging
from src.log_utils import setup_logger
from src.blob_utils import parse_blob_path, upload_blob, load_text_blob

logger = setup_logger(__name__, logging.DEBUG)

def rate_limiter_entity(context: df.DurableEntityContext, config: dict):
    """Generic Durable Entity that implements token bucket rate limiting for different workflows."""
    logger.debug(f"Calling rate limiter")
    
    entity_name = context.entity_key
    
    # Get configuration for this entity
    for workflow, workflow_config in config.items():
        if workflow_config["entity_name"] == entity_name:
            config = workflow_config
            break

    if not config:
        logger.warning(f"Unknown rate limiter entity key {entity_name}")
        context.set_result(False)
        return
    
    rate_limit_rpm = config["rate_limit_rpm"]
    
    current_state = context.get_state(lambda: {
        "tokens": rate_limit_rpm,
        "last_refill": None,
    })
    
    current_time_str = context.get_input()
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
        
    operation = context.operation_name
    
    if operation == "try_consume":
        if current_state["tokens"] > 0:
            current_state["tokens"] -= 1
            context.set_result(True)
        else:
            context.set_result(False)
    
    elif operation == "get_status":
        context.set_result(current_state)
    
    context.set_state(current_state)
    logger.debug(f"Rate limiter finished with result: {context._result} and state: {context.get_state()}")


# Shared Orchestrator Function
def orchestrator_logic(context: df.DurableOrchestrationContext, config: dict, input_data: dict):
    """Generic Orchestrator that enforces rate limiting using the durable entity.."""
    input_data = context.get_input()
    workflow_type = input_data.get("workflow_type")
    
    if workflow_type not in config:
        raise ValueError(f"Unknown workflow type: {workflow_type}")
    
    config = config[workflow_type]
    entity_id = df.EntityId("generic_rate_limiter_entity", config["entity_name"])
    
    logger.debug(f"Executing orchestrator logic -> entity: {config['entity_name']}")

    # Wait for rate limit token
    while True:
        logger.debug("Checking rate limiter")
        allowed = yield context.call_entity(entity_id, "try_consume", context.current_utc_datetime.isoformat())
        if allowed:
            break
        # Wait before retrying
        retry_time = context.current_utc_datetime + timedelta(seconds=5)
        yield context.create_timer(retry_time)
    
    logger.debug("Orchestrator passed rate limiter and calling next activity: %s", config["activity_name"])
    # Process the blob with acquired rate token
    result = yield context.call_activity(config["activity_name"], input_data)
    return result

