# RUN THIS from azure root via $ python -m tests.test_rate_limiter

from datetime import datetime, timezone, timedelta
from tests.test_orchestration import TEST_CONFIG, MockOrchestrationContext, run_orchestrator

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
        orch_context = MockOrchestrationContext("test_workflow", item, entity_state, {})
        
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