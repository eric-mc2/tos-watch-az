import logging
from src.log_utils import setup_logger
import azure.functions as func
from azure import durable_functions as df
import json
from datetime import datetime, timezone


logger = setup_logger(__name__, logging.INFO)

async def circuit_breaker_entity(context: df.DurableEntityContext):
    """Circuit breaker entity to halt all processing on systemic failures."""
    # Always initialize with default state if None
    current_state = context.get_state(lambda: {
        "strikes": 3,
        "is_open": False,
        "error_message": None,
        "opened_at": None
    })
    
    # Ensure state is a dict (handle edge cases)
    if not isinstance(current_state, dict):
        current_state = {
            "strikes": 3,
            "is_open": False,
            "error_message": None,
            "opened_at": None
        }
    
    operation = context.operation_name
    
    if operation == "trip":
        # Open the circuit breaker
        input_data = context.get_input()
        error_msg = input_data if isinstance(input_data, str) else str(input_data)
        strikes = max(0, current_state.get("strikes") - 1)
        is_open = strikes == 0
        current_state["strikes"] = strikes
        current_state["is_open"] = is_open
        current_state["error_message"] = error_msg if is_open else None
        current_state["opened_at"] = datetime.now(timezone.utc).isoformat() if is_open else None
        if is_open:
            logger.error(f"Circuit breaker tripped: {error_msg}")
        context.set_result(True)
        
    elif operation == "reset":
        # Close the circuit breaker
        current_state["strikes"] = 3
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
    

async def check_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Check a circuit breaker for a workflow type."""
    if 'workflow_type' not in req.params:
        return func.HttpResponse("workflow_type not specified in http query string", status_code=400)
    
    workflow_type = req.params.get('workflow_type')
    entity_id = df.EntityId("circuit_breaker_entity_func", workflow_type)
    
    # Check if entity exists first
    entity_state = await client.read_entity_state(entity_id)
    if not entity_state.entity_exists:
        return func.HttpResponse(
            f"Circuit breaker for {workflow_type} doesn't exist yet (no orchestrations have run)", 
            status_code=200
        )
    
    # Entity exists, get its current state directly
    state = entity_state.entity_state
    
    # if not state:
    #     # Should never run
    #     state = {"is_open": False, "error_message": None, "opened_at": None}
    
    status_msg = "tripped" if state.get('is_open', False) else "running"
    
    response_data = {
        "workflow_type": workflow_type,
        "status": status_msg,
        "is_open": state.get('is_open', False),
        "error_message": state.get('error_message'),
        "opened_at": state.get('opened_at')
    }
    
    logger.info(f"Circuit breaker status for {workflow_type}: {status_msg}")
    return func.HttpResponse(
        json.dumps(response_data, indent=2),
        mimetype="application/json",
        status_code=200
    )


async def reset_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Manually reset a circuit breaker for a workflow type."""
    if 'workflow_type' not in req.params:
        return func.HttpResponse("workflow_type not specified in http query string", status_code=400)
    
    workflow_type = req.params.get('workflow_type')
    entity_id = df.EntityId("circuit_breaker_entity_func", workflow_type)
   
    # Check if entity exists first
    entity_state = await client.read_entity_state(entity_id) 
    if not entity_state.entity_exists:
        return func.HttpResponse(
            f"Circuit breaker for {workflow_type} doesn't exist yet (no orchestrations have run)", 
            status_code=200
        )
    
    # Entity exists, signal it to reset
    entity_id = df.EntityId("circuit_breaker_entity_func", workflow_type)
    await client.signal_entity(entity_id, "reset")
    
    logger.info(f"Circuit breaker reset for workflow: {workflow_type}")
    return func.HttpResponse(f"Circuit breaker reset for {workflow_type}", status_code=200)

