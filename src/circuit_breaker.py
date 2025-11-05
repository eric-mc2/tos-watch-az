import logging
import azure.functions as func
from azure import durable_functions as df
import json
from datetime import datetime, timezone
from src.log_utils import setup_logger

logger = setup_logger(__name__, logging.INFO)

TRIP = "TRIP"
RESET = "RESET"
GET_STATUS = "GET_STATUS"

def circuit_breaker_entity(context: df.DurableEntityContext):
    """Circuit breaker entity to halt all processing on systemic failures."""
    # Always initialize with default state if None
    current_state = context.get_state(lambda: {
        "strikes": 3,
        "is_open": False,
        "error_message": None,
        "opened_at": None
    })
    
    operation = context.operation_name
    
    if operation == TRIP:
        # Open the circuit breaker
        input_data = context.get_input()
        error_msg = str(input_data) # When we call trip the input should be an error message
        strikes = max(0, current_state.get("strikes") - 1)
        is_open = strikes == 0
        current_state["strikes"] = strikes
        current_state["is_open"] = is_open
        current_state["error_message"] = error_msg if is_open else None
        current_state["opened_at"] = datetime.now(timezone.utc).isoformat() if is_open else None
        if is_open:
            logger.error(f"Circuit breaker tripped: {error_msg}")
        context.set_result(not is_open)
        
    elif operation == RESET:
        # Close the circuit breaker
        current_state["strikes"] = 3
        current_state["is_open"] = False
        current_state["error_message"] = None
        current_state["opened_at"] = None
        logger.info("Circuit breaker reset")
        context.set_result(True)
        
    elif operation == GET_STATUS:
        # Check if circuit is open
        context.set_result(not current_state['is_open'])
    else:
        raise ValueError("Unknown operation {operation}")
    
    context.set_state(current_state)
    

async def check_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Check a circuit breaker for a workflow type."""
    if 'workflow_type' not in req.params:
        return func.HttpResponse("workflow_type not specified in http query string", status_code=400)
    
    workflow_type = req.params.get('workflow_type')
    entity_id = df.EntityId("circuit_breaker", workflow_type)
    
    # Check if entity exists first
    entity_state = await client.read_entity_state(entity_id)
    if not entity_state.entity_exists:
        return func.HttpResponse(
            f"Circuit breaker for {workflow_type} doesn't exist yet (no orchestrations have run)", 
            status_code=200
        )
    
    # Entity exists, get its current state directly
    state = entity_state.entity_state
    
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
    entity_id = df.EntityId("circuit_breaker", workflow_type)
   
    # Check if entity exists first
    entity_state = await client.read_entity_state(entity_id) 
    if not entity_state.entity_exists:
        return func.HttpResponse(
            f"Circuit breaker for {workflow_type} doesn't exist yet (no orchestrations have run)", 
            status_code=200
        )
    
    # Entity exists, signal it to reset
    entity_id = df.EntityId("circuit_breaker", workflow_type)
    await client.signal_entity(entity_id, "reset")
    
    logger.info(f"Circuit breaker reset for workflow: {workflow_type}")
    return func.HttpResponse(f"Circuit breaker reset for {workflow_type}", status_code=200)

