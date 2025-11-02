# RUN THIS from azure root via $ python -m tests.test_rate_limiter

from datetime import datetime, timezone
from tests.test_orchestration import TEST_CONFIG, MockOrchestrationContext, run_orchestrator

def test_circuit_breaker():
    """Test circuit breaker by submitting many tasks, failing some, and showing cancellation"""
    print("\n" + "="*60)
    print("CIRCUIT BREAKER TEST - SYSTEMIC FAILURE DETECTION")
    print("="*60 + "\n")
    print("Submitting 20 orchestrations concurrently.")
    print("Items 5-7 will fail with FATAL errors.")
    print("Expected: Circuit trips, remaining items are rejected.\n")
    
    # Shared circuit breaker state (simulates durable storage)
    circuit_breaker_state = {
        "is_open": False,
        "error_message": None,
        "opened_at": None
    }
    
    # Rate limiter state (set high limit so it doesn't interfere)
    entity_state = {
        "tokens": 1000,
        "last_refill": None
    }
    
    # Create config with high rate limit
    circuit_test_config = {
        "test_workflow": {
            "rate_limit_rpm": 1000,  # High limit so rate limiter doesn't interfere
            "activity_name": "test_processor",
            "max_retries": 2
        }
    }
    
    items = [f"task_{i:02d}" for i in range(20)]
    start_time = datetime.now(timezone.utc)
    
    # Items that will fail with fatal errors
    fatal_failure_items = {4, 5, 6}  # 0-indexed, so items 5-7 in display
    
    for i, item in enumerate(items, 0):
        item_start = datetime.now(timezone.utc)
        
        # Determine if this item should fail fatally
        fail_count = 0
        if i in fatal_failure_items:
            fail_count = 999  # Always fail with non-retryable error
        
        # Create orchestration context
        orch_context = MockOrchestrationContext(
            "test_workflow", 
            item, 
            entity_state, 
            circuit_breaker_state,
            fail_count=fail_count
        )
        
        # Override call_activity to simulate fatal vs retryable errors
        original_call_activity = orch_context.call_activity
        def call_activity_with_fatal(activity_name, input_data):
            if i in fatal_failure_items:
                # Fatal error - will trip circuit breaker
                raise Exception("FATAL: Database schema mismatch - incompatible data format")
            return original_call_activity(activity_name, input_data)
        
        orch_context.call_activity = call_activity_with_fatal
        
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        try:
            # Run the orchestrator
            result = run_orchestrator(orch_context, circuit_test_config)
            
            if orch_context.activity_called:
                print(f"✓ [{elapsed:6.2f}s] Task {i+1:2d}/20: {item:10s} - PROCESSED")
            else:
                print(f"⚠ [{elapsed:6.2f}s] Task {i+1:2d}/20: {item:10s} - SKIPPED (no activity called)")
                
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a circuit breaker rejection
            if "Circuit breaker open" in error_msg:
                print(f"🚫 [{elapsed:6.2f}s] Task {i+1:2d}/20: {item:10s} - REJECTED (circuit open)")
            elif "FATAL" in error_msg:
                print(f"💥 [{elapsed:6.2f}s] Task {i+1:2d}/20: {item:10s} - FAILED (fatal error, tripping circuit)")
                print(f"\n{'─'*60}")
                print(f"⚡ CIRCUIT BREAKER TRIPPED!")
                print(f"   Reason: {circuit_breaker_state.get('error_message', 'Unknown')}")
                print(f"   All subsequent tasks will be rejected")
                print(f"{'─'*60}\n")
            else:
                print(f"✗ [{elapsed:6.2f}s] Task {i+1:2d}/20: {item:10s} - ERROR: {error_msg}")
        
        # Small delay for readability
        import time
        time.sleep(0.05)
    
    print(f"\n{'='*60}")
    print(f"Test completed!")
    print(f"Total elapsed: {(datetime.now(timezone.utc) - start_time).total_seconds():.2f}s")
    print(f"{'='*60}\n")
    
    # Show final circuit breaker state
    print("Final circuit breaker state:")
    print(f"  Is Open: {circuit_breaker_state['is_open']}")
    print(f"  Error: {circuit_breaker_state.get('error_message', 'None')}")
    print(f"  Opened At: {circuit_breaker_state.get('opened_at', 'N/A')}")
    print()


if __name__ == "__main__":
    test_circuit_breaker()