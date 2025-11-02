from azure import durable_functions as df
from datetime import datetime, timedelta, timezone
import logging
from src.log_utils import setup_logger

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
    

def circuit_breaker_entity(context: df.DurableEntityContext):
    """Circuit breaker entity to halt all processing on systemic failures."""
    # Always initialize with default state if None
    current_state = context.get_state(lambda: {
        "is_open": False,
        "error_message": None,
        "opened_at": None
    })
    
    # Ensure state is a dict (handle edge cases)
    if not isinstance(current_state, dict):
        current_state = {
            "is_open": False,
            "error_message": None,
            "opened_at": None
        }
    
    operation = context.operation_name
    
    if operation == "trip":
        # Open the circuit breaker
        input_data = context.get_input()
        error_msg = input_data if isinstance(input_data, str) else str(input_data)
        current_state["is_open"] = True
        current_state["error_message"] = error_msg
        current_state["opened_at"] = datetime.now(timezone.utc).isoformat()
        logger.error(f"Circuit breaker tripped: {error_msg}")
        context.set_result(True)
        
    elif operation == "reset":
        # Close the circuit breaker
        current_state["is_open"] = False
        current_state["error_message"] = None
        current_state["opened_at"] = None
        logger.info("Circuit breaker reset")
        context.set_result(True)
        
    elif operation == "get_status":
        # Check if circuit is open
        context.set_result(current_state)
    
    else:
        # Unknown operation
        logger.warning(f"Unknown operation '{operation}' for circuit breaker entity")
        context.set_result(None)
    
    context.set_state(current_state)


def is_retryable_error(error_msg: str) -> bool:
    """Determine if an error is retryable (transient) or fatal (systemic)."""
    retryable_patterns = [
        "Max retries exceeded",
        "Connection refused",
        "Connection reset",
        "Timeout",
        "Too Many Requests",
        "429",
        "503",
        "504",
        "Connection pool is full",
    ]
    
    return any(pattern.lower() in error_msg.lower() for pattern in retryable_patterns)


def orchestrator_logic(context: df.DurableOrchestrationContext, config: dict):
    """Generic Orchestrator that enforces rate limiting using the durable entity."""
    input_data = context.get_input()
    workflow_type = input_data.get("workflow_type")
    blob_name = input_data.get("blob_name")
    
    if workflow_type not in config:
        raise ValueError(f"Unknown workflow type: {workflow_type}")
    
    workflow_config = config[workflow_type]
    rate_limiter_id = df.EntityId("generic_rate_limiter_entity", workflow_type)
    circuit_breaker_id = df.EntityId("circuit_breaker_entity_func", workflow_type)    

    # Check circuit breaker first
    logger.debug(f"Checking circuit breaker {workflow_type} workflow for {blob_name}")
    circuit_status = yield context.call_entity(circuit_breaker_id, "get_status")
    if circuit_status.get("is_open", False):
        error_msg = circuit_status.get("error_message", "Circuit breaker is open")
        logger.warning(f"Circuit breaker open for {workflow_type}, aborting: {error_msg}")
        raise Exception(f"Circuit breaker open: {error_msg}")

    # Wait for rate limit token
    while True:
        # Pass config and time to entity
        entity_input = {
            "config": workflow_config,
            "current_time": context.current_utc_datetime.isoformat()
        }
        logger.debug(f"Checking rate limiter {workflow_type} workflow for {blob_name}")
        allowed = yield context.call_entity(rate_limiter_id, "try_consume", entity_input)
        if allowed:
            break
        # Wait before retrying
        logger.warning(f"Throttling processing input: {blob_name}")
        delay = workflow_config.get("delay", 5)
        retry_time = context.current_utc_datetime + timedelta(seconds=delay)
        yield context.create_timer(retry_time)
    
    logger.debug("Orchestrator passed rate limiter and calling activity: %s", workflow_config["activity_name"])
    
    # Retry logic with exponential backoff
    max_retries = workflow_config.get("max_retries", 3)
    retry_count = 0
    base_delay = 10  # seconds
    
    while retry_count <= max_retries:
        try:
            result = yield context.call_activity(workflow_config["activity_name"], input_data)
            logger.info(f"Successfully processed {blob_name}")
            return result
            
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"Activity failed (attempt {retry_count + 1}/{max_retries + 1}): {error_msg}")
            
            # Check if this is a retryable error
            if not is_retryable_error(error_msg):
                # Fatal error - trip circuit breaker to stop all pending work
                logger.error(f"Non-retryable error encountered, tripping circuit breaker: {error_msg}")
                yield context.call_entity(circuit_breaker_id, "trip", error_msg)
                raise
            
            # Retryable error
            if retry_count >= max_retries:
                logger.error(f"Max retries ({max_retries}) exceeded for {blob_name}")
                raise
            
            # Exponential backoff with jitter
            delay = base_delay * (2 ** retry_count)
            logger.info(f"Retrying in {delay}s...")
            retry_time = context.current_utc_datetime + timedelta(seconds=delay)
            yield context.create_timer(retry_time)
            retry_count += 1
    
    raise Exception("Exhausted all retries")

