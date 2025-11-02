# RUN THIS from azure root via $ python -m tests.test_rate_limiter

from datetime import datetime, timezone, timedelta
import logging
from src.rate_limiter import rate_limiter_entity, orchestrator_logic
from src.log_utils import setup_logger


logger = setup_logger(__name__, logging.INFO)

# Test configuration with low rate limit to see throttling
TEST_CONFIG = {
    "test_workflow": {
        "rate_limit_rpm": 5,  # Only 5 requests per minute
        "entity_name": "test_rate_limiter",
        "orchestrator_name": "test_orchestrator",
        "activity_name": "test_processor"
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
    def __init__(self, workflow_type, blob_name, entity_state):
        self._input = {
            "workflow_type": workflow_type,
            "blob_name": blob_name
        }
        self.current_utc_datetime = datetime.now(timezone.utc)
        self.entity_state = entity_state
        self.activity_called = False
        self.activity_result = None
        
    def get_input(self):
        return self._input
    
    def call_entity(self, entity_id, operation, input_data):
        """Simulate calling the entity"""
        # Extract entity key from EntityId (second parameter)
        entity_key = entity_id.entity_key if hasattr(entity_id, 'entity_key') else entity_id
        
        # Create entity context and call the rate limiter
        entity_context = MockEntityContext(entity_key, operation, input_data)
        entity_context._state = self.entity_state
        
        rate_limiter_entity(entity_context, TEST_CONFIG)
        
        # Update shared state
        self.entity_state.update(entity_context.get_state())
        
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
        """Simulate calling an activity"""
        self.activity_called = True
        self.activity_result = f"Processed: {input_data['blob_name']}"
        return self.activity_result

def run_orchestrator(context, config):
    """Helper to run the orchestrator generator to completion"""
    gen = orchestrator_logic(context, config, context.get_input())
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

def test_rate_limiter():
    """Test the rate limiter by processing multiple items and showing timestamps"""
    print("\n" + "="*60)
    print("RATE LIMITER TEST - 5 RPM LIMIT")
    print("="*60 + "\n")
    print("Processing 10 items with a 5 requests/minute rate limit.")
    print("Expected: First 5 should process immediately, then delays.\n")
    
    # Shared entity state (simulates durable storage)
    entity_state = {
        "tokens": 5,
        "last_refill": None
    }
    
    items = [f"item_{i:02d}" for i in range(10)]
    start_time = datetime.now(timezone.utc)
    
    for i, item in enumerate(items, 1):
        item_start = datetime.now(timezone.utc)
        
        # Create orchestration context
        orch_context = MockOrchestrationContext("test_workflow", item, entity_state)
        
        # Actually call orchestrator_logic
        result = run_orchestrator(orch_context, TEST_CONFIG)
        
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        tokens = entity_state['tokens']
        
        if orch_context.activity_called:
            print(f"✓ [{elapsed:6.2f}s] Item {i:2d}/10: {item:10s} - PROCESSED (tokens left: {tokens})")
        else:
            print(f"✗ [{elapsed:6.2f}s] Item {i:2d}/10: {item:10s} - FAILED")
        
        # Show refill events
        if i == 5:
            print(f"\n--- Token bucket empty, will refill after 60 seconds ---\n")
            # Fast-forward time to trigger refill
            orch_context.current_utc_datetime += timedelta(seconds=60)
        
        # Small delay to make output more readable
        import time
        time.sleep(0.1)
    
    print(f"\n{'='*60}")
    print(f"Test completed!")
    print(f"Total elapsed: {(datetime.now(timezone.utc) - start_time).total_seconds():.2f}s")
    print(f"{'='*60}\n")
    
    # Show final state
    print("Final entity state:")
    print(f"  Tokens remaining: {entity_state['tokens']}")
    print(f"  Last refill: {entity_state['last_refill']}")

if __name__ == "__main__":
    test_rate_limiter()
