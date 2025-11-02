from datetime import datetime, timezone
from src.rate_limiter import rate_limiter_entity, orchestrator_logic, circuit_breaker_entity, is_retryable_error
import logging
from src.log_utils import setup_logger

logger = setup_logger(__name__, logging.INFO)

# Test configuration with low rate limit to see throttling
TEST_CONFIG = {
    "test_workflow": {
        "rate_limit_rpm": 5,  # Only 5 requests per minute
        "activity_name": "test_processor",
        "max_retries": 2
    }
}

class MockEntityContext:
    """Mock DurableEntityContext for testing the entity"""
    def __init__(self, entity_key, operation_name, input_data=None):
        self.entity_key = entity_key
        self.operation_name = operation_name
        self._input = input_data
        self._state = None
        self._result = None
        
    def get_state(self, default_factory=None):
        if self._state is None and default_factory:
            self._state = default_factory()
        return self._state
    
    def set_state(self, state):
        self._state = state
    
    def get_input(self):
        return self._input
    
    def set_result(self, result):
        self._result = result

class MockOrchestrationContext:
    """Mock DurableOrchestrationContext for testing the orchestrator"""
    def __init__(self, workflow_type, blob_name, rate_limiter_state, circuit_breaker_state, fail_count=0):
        self._input = {
            "workflow_type": workflow_type,
            "blob_name": blob_name
        }
        self.current_utc_datetime = datetime.now(timezone.utc)
        self.rate_limiter_state = rate_limiter_state
        self.circuit_breaker_state = circuit_breaker_state
        self.activity_called = False
        self.activity_result = None
        self.fail_count = fail_count
        self.call_count = 0
        
    def get_input(self):
        return self._input
    
    def call_entity(self, entity_id, operation, input_data=None):
        """Simulate calling the entity"""
        entity_name = entity_id.entity_name if hasattr(entity_id, 'entity_name') else str(entity_id).split(',')[0]
        entity_key = entity_id.entity_key if hasattr(entity_id, 'entity_key') else entity_id
        
        # Handle circuit breaker entity
        if entity_name == "circuit_breaker_entity":
            entity_context = MockEntityContext(entity_key, operation, input_data)
            entity_context._state = self.circuit_breaker_state
            circuit_breaker_entity(entity_context)
            self.circuit_breaker_state.update(entity_context.get_state())
            return entity_context._result
        
        # Handle rate limiter entity
        entity_context = MockEntityContext(entity_key, operation, input_data)
        entity_context._state = self.rate_limiter_state
        rate_limiter_entity(entity_context, TEST_CONFIG)
        self.rate_limiter_state.update(entity_context.get_state())
        return entity_context._result
    
    def create_timer(self, fire_at):
        """Simulate timer delay - actually sleep to show throttling"""
        import time
        # Calculate how long to sleep (in real deployment, orchestrator would pause here)
        delay = (fire_at - self.current_utc_datetime).total_seconds()
        logger.debug(f"Timer created, sleeping for {delay}s")
        time.sleep(min(delay, 1))  # Cap at 1 second for demo purposes
        self.current_utc_datetime = fire_at
        return "TIMER_COMPLETED"
    
    def call_activity(self, activity_name, input_data):
        """Simulate calling an activity with optional failures"""
        self.activity_called = True
        self.call_count += 1
        
        # Simulate transient failures
        if self.call_count <= self.fail_count:
            raise Exception("Max retries exceeded with url: Connection refused")
        
        self.activity_result = f"Processed: {input_data['blob_name']}"
        return self.activity_result

def run_orchestrator(context, config):
    """Helper to run the orchestrator generator to completion"""
    gen = orchestrator_logic(context, config)
    result = None
    
    try:
        while True:
            # Send the previous result back into the generator
            yielded = gen.send(result)
            
            # If it's a timer, it has already slept in create_timer
            if yielded == "TIMER_COMPLETED":
                result = None
            else:
                # Otherwise it's a result from call_entity or call_activity
                result = yielded
                
    except StopIteration as e:
        # The generator is done, return its final value
        return e.value