import logging
import json
import asyncio
from datetime import datetime, timezone
import azure.functions as func
from azure import durable_functions as df
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
        logger.info("Circuit breaker entity received reset signal.")
        current_state["strikes"] = 3
        current_state["is_open"] = False
        current_state["error_message"] = None
        current_state["opened_at"] = None
        context.set_result(True)
        
    elif operation == GET_STATUS:
        # Check if circuit is open
        context.set_result(not current_state['is_open'])
    else:
        raise ValueError(f"Unknown operation {operation}")
    
    context.set_state(current_state)
    

async def check_circuit_breaker(workflow_type: str, client: df.DurableOrchestrationClient) -> dict:
    """Check a circuit breaker for a workflow type."""
    entity_id = df.EntityId("circuit_breaker", workflow_type)
    
    # Check if entity exists first
    entity = await client.read_entity_state(entity_id)
    if not entity.entity_exists or entity.entity_state is None:
        return f"Circuit breaker for [{workflow_type}] doesn't exist yet (no orchestrations have run)"
    
    # Entity exists, get its current state directly
    state = entity.entity_state
    
    status_msg = "tripped" if state.get('is_open', False) else f"running ({state.get('strikes')}/3 strikes left)"
    
    response_data = {
        "workflow_type": workflow_type,
        "status": status_msg,
        "is_open": state.get('is_open', False),
        "error_message": state.get('error_message'),
        "opened_at": state.get('opened_at')
    }
    
    logger.info(f"Circuit breaker check status: [{workflow_type}] {status_msg}")
    return response_data


async def reset_circuit_breaker(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    """Manually reset a circuit breaker for a workflow type."""
    if 'workflow_type' not in req.params:
        return func.HttpResponse("workflow_type not specified in http query string", status_code=400)
    
    workflow_type = req.params.get('workflow_type')
    
    # Whether it exists or not, we can signal it to reset.
    entity_id = df.EntityId("circuit_breaker", workflow_type)
    await client.signal_entity(entity_id, RESET)

    # Wait with exponential backoff for reset confirmation
    confirmed = False
    max_attempts = 10
    for attempt in range(max_attempts):
        logger.debug(f"Waiting for circuit reset confirmation ({attempt}/{max_attempts}) for [{workflow_type}]")
        await asyncio.sleep(0.1 * (2 ** attempt))  # 100ms, 200ms, 400ms, etc.
        status = await check_circuit_breaker(workflow_type, client)
        confirmed = not status['is_open']
        if confirmed:
            break
    
    if not confirmed:
        return func.HttpResponse(f"Circuit breaker reset timed out for [{workflow_type}]", status_code=500)

    tasks = await list_tasks(client, workflow_type, [df.OrchestrationRuntimeStatus.Running])
    logger.info(f"Found {len(tasks)} orchestrators to wake for [{workflow_type}].")
    for task in tasks:
        task_id = task['data'].get("task_id", "undefined")
        logger.debug(f"Re-submitting cancelled task [{workflow_type}] {task_id}")
        try:
            await client.raise_event(task['instance_id'], RESET)
        except Exception as e:
            # XXX: This sometimes fails, presumably because task is already completed?
            logger.error(f"Failed to re-submit cancelled task [{workflow_type}] {task_id}: {e}")

    logger.info(f"Circuit breaker reset for [{workflow_type}]")
    return func.HttpResponse(f"Circuit breaker reset for [{workflow_type}]", status_code=200)


async def list_tasks(client: df.DurableOrchestrationClient, workflow_type: str, status: list[df.OrchestrationRuntimeStatus]):
    if isinstance(status, df.OrchestrationRuntimeStatus):
        status = [status]
    tasks = await client.get_status_by(runtime_status=status)
    relevant_tasks = []
    for task in tasks:
        # According to https://learn.microsoft.com/en-us/python/api/azure-functions-durable/azure.durable_functions.models.durableorchestrationstatus.durableorchestrationstatus?view=azure-python
        # it is input_ not _input
        input_data = task.input_
        data = None
        if isinstance(input_data, str):
            try:
                data = json.loads(input_data)
            except json.JSONDecodeError as e:
                pass
        elif isinstance(input_data, dict):
            data = input_data
        
        if data is None:
            logger.warning("Unknown input data from %s: ", workflow_type, input_data)
            continue
        elif not isinstance(data, dict):
            raise ValueError("Unexpected input data %s: %s", type(data), data)
        elif "workflow_type" not in data:
            continue  # it's an entity
        elif data['workflow_type'] != workflow_type:
            continue  # orchestrator or entity but irrelevant workflow
        
        relevant_tasks.append(dict(instance_id = task.instance_id, 
                                   status = task.runtime_status,
                                   created = task.created_time,
                                   updated = task.last_updated_time,
                                   data = data))
    
    return relevant_tasks
