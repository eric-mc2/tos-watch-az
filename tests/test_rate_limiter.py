"""
Direct tests for the rate_limiter_entity function.
Tests entity logic in isolation using MockEntityContext.
"""
import unittest
from datetime import datetime, timezone, timedelta
import time


class MockEntityContext:
    """Mock DurableEntityContext for testing the rate limiter entity"""
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


class TestRateLimiterEntity(unittest.TestCase):
    """Test the rate limiter entity directly"""
    
    def setUp(self):
        """Set up test configuration"""
        self.workflow_config = {
            "rate_limit_rpm": 6,  # 6 requests per minute
            "delay": 0.1,
            "activity_name": "test_processor",
            "max_retries": 2
        }
        
        self.config = {
            "test_workflow": self.workflow_config
        }
    
    def _create_input_data(self, current_time=None):
        """Helper to create input_data structure expected by entity"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        return {
            "config": self.workflow_config,
            "current_time": current_time.isoformat()
        }
    
    def test_token_acquisition_until_exhaustion(self):
        """Test that tokens are consumed until exhausted"""
        print("\n" + "="*60)
        print("TEST: Token Acquisition Until Exhaustion")
        print("="*60)
        
        from src.rate_limiter import rate_limiter_entity
        
        # Initial state with 6 tokens
        current_time = datetime.now(timezone.utc)
        state = {
            "tokens": 6,
            "last_refill": current_time.isoformat()
        }
        
        results = []
        
        # Acquire tokens 10 times
        for i in range(10):
            input_data = self._create_input_data(current_time)
            context = MockEntityContext("test_workflow", "try_consume", input_data)
            context._state = state
            
            rate_limiter_entity(context, self.config)
            
            result = context._result
            results.append(result)
            state = context.get_state()
            
            print(f"  Request {i+1:2d}: {'✓ ALLOWED' if result else '✗ DENIED'} (tokens: {state['tokens']})")
        
        # Assertions
        # First 6 should succeed
        self.assertTrue(all(results[:6]), "First 6 requests should be allowed")
        # Remaining 4 should fail
        self.assertFalse(any(results[6:]), "Requests 7-10 should be denied")
        # Token count should be 0
        self.assertEqual(state['tokens'], 0, "All tokens should be exhausted")
        
        print(f"\n✓ Test passed: 6 allowed, 4 denied, 0 tokens remaining\n")
    
    def test_refill_after_time_window(self):
        """Test that tokens refill after the time window expires"""
        print("\n" + "="*60)
        print("TEST: Token Refill After Time Window")
        print("="*60)
        
        from src.rate_limiter import rate_limiter_entity
        
        # Start with exhausted state
        initial_time = datetime.now(timezone.utc)
        state = {
            "tokens": 0,
            "last_refill": initial_time.isoformat()
        }
        
        # Try to consume - should fail
        input_data = self._create_input_data(initial_time)
        context = MockEntityContext("test_workflow", "try_consume", input_data)
        context._state = state
        rate_limiter_entity(context, self.config)
        
        self.assertFalse(context._result, "Should be denied when tokens exhausted")
        print(f"  At t=0s: ✗ DENIED (tokens: {state['tokens']})")
        
        # Advance time by 59 seconds (not enough for refill)
        time_59s = initial_time + timedelta(seconds=59)
        input_data = self._create_input_data(time_59s)
        context = MockEntityContext("test_workflow", "try_consume", input_data)
        context._state = state
        rate_limiter_entity(context, self.config)
        
        self.assertFalse(context._result, "Should still be denied at 59 seconds")
        print(f"  At t=59s: ✗ DENIED (tokens: {context.get_state()['tokens']}) - not enough time")
        
        # Advance time by 61 seconds (should trigger refill)
        time_61s = initial_time + timedelta(seconds=61)
        input_data = self._create_input_data(time_61s)
        context = MockEntityContext("test_workflow", "try_consume", input_data)
        context._state = state
        rate_limiter_entity(context, self.config)
        
        state = context.get_state()
        self.assertTrue(context._result, "Should be allowed after refill")
        self.assertEqual(state['tokens'], 5, "Should have 5 tokens remaining after consuming 1")
        print(f"  At t=61s: ✓ ALLOWED (tokens: {state['tokens']}) - refilled!")
        
        print(f"\n✓ Test passed: Refill triggered after 60+ seconds\n")
    
    def test_burst_handling(self):
        """Test behavior during burst of requests"""
        print("\n" + "="*60)
        print("TEST: Burst Handling")
        print("="*60)
        
        from src.rate_limiter import rate_limiter_entity
        
        current_time = datetime.now(timezone.utc)
        state = {
            "tokens": 3,
            "last_refill": current_time.isoformat()
        }
        
        # Burst of 5 requests
        print("\n  Initial burst (3 tokens available):")
        for i in range(5):
            input_data = self._create_input_data(current_time)
            context = MockEntityContext("test_workflow", "try_consume", input_data)
            context._state = state
            rate_limiter_entity(context, self.config)
            
            result = context._result
            state = context.get_state()
            print(f"    Request {i+1}: {'✓ ALLOWED' if result else '✗ DENIED'} (tokens: {state['tokens']})")
        
        self.assertEqual(state['tokens'], 0, "All tokens should be consumed")
        
        # Wait for refill
        print("\n  Simulating 61 seconds passing for refill...")
        refill_time = current_time + timedelta(seconds=61)
        
        # Another burst
        print("\n  Second burst (after refill):")
        for i in range(8):
            input_data = self._create_input_data(refill_time)
            context = MockEntityContext("test_workflow", "try_consume", input_data)
            context._state = state
            rate_limiter_entity(context, self.config)
            
            result = context._result
            state = context.get_state()
            print(f"    Request {i+1}: {'✓ ALLOWED' if result else '✗ DENIED'} (tokens: {state['tokens']})")
        
        # After refill, we should have gotten 6 more tokens, consumed all 6, 2 denied
        self.assertEqual(state['tokens'], 0, "Should have consumed all refilled tokens")
        
        print(f"\n✓ Test passed: Burst handled correctly with refill\n")
    
    def test_get_status_operation(self):
        """Test the get_status operation"""
        print("\n" + "="*60)
        print("TEST: Get Status Operation")
        print("="*60)
        
        from src.rate_limiter import rate_limiter_entity
        
        current_time = datetime.now(timezone.utc)
        state = {
            "tokens": 3,
            "last_refill": current_time.isoformat()
        }
        
        # get_status doesn't need input_data
        context = MockEntityContext("test_workflow", "get_status", None)
        context._state = state
        rate_limiter_entity(context, self.config)
        
        status = context._result
        
        print(f"  Status: {status}")
        
        self.assertIsNotNone(status, "Status should be returned")
        self.assertEqual(status['tokens'], 3, "Status should show current tokens")
        self.assertIn('last_refill', status, "Status should include last_refill")
        
        print(f"\n✓ Test passed: Status operation works correctly\n")
    
    def test_real_time_throttling_visual(self):
        """Integration test with real time delays to visually observe throttling"""
        print("\n" + "="*60)
        print("TEST: Real-Time Throttling (Visual Integration Test)")
        print("="*60)
        print("\nThis test uses REAL TIME with visible pauses.")
        print("Configuration: 3 requests per minute (20 seconds per token refill)")
        print("Processing 10 requests...\n")
        
        from src.rate_limiter import rate_limiter_entity
        
        # Low RPM for noticeable effect: 3 requests per minute
        visual_config = {
            "rate_limit_rpm": 3,  # Only 3 requests per minute
            "delay": 0.5,
            "activity_name": "visual_processor",
            "max_retries": 2
        }
        
        test_config = {
            "visual_workflow": visual_config
        }
        
        # Start with full tokens
        start_time = datetime.now(timezone.utc)
        state = {
            "tokens": 3,
            "last_refill": start_time.isoformat()
        }
        
        allowed_count = 0
        denied_count = 0
        
        print(f"Start time: {start_time.strftime('%H:%M:%S')}")
        print(f"Initial tokens: {state['tokens']}\n")
        
        # Process 10 requests with real time
        for i in range(10):
            current_time = datetime.now(timezone.utc)
            elapsed = (current_time - start_time).total_seconds()
            
            input_data = {
                "config": visual_config,
                "current_time": current_time.isoformat()
            }
            
            context = MockEntityContext("visual_workflow", "try_consume", input_data)
            context._state = state
            rate_limiter_entity(context, test_config)
            
            result = context._result
            state = context.get_state()
            
            status = "✓ ALLOWED" if result else "✗ DENIED "
            print(f"[{elapsed:6.1f}s] Request {i+1:2d}: {status} | Tokens: {state['tokens']} | Time: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
            
            if result:
                allowed_count += 1
            else:
                denied_count += 1
            
            # Small delay between requests to make it more realistic
            time.sleep(0.2)
            
            # After exhausting tokens, wait for a refill cycle
            if state['tokens'] == 0 and i < 9:
                print(f"\n⏸️  THROTTLED - Waiting for token refill (20 seconds)...")
                time.sleep(21)  # Wait just over 20 seconds (60s / 3 RPM)
                print(f"⏯️  RESUMING - Tokens should be refilled\n")
        
        end_time = datetime.now(timezone.utc)
        total_duration = (end_time - start_time).total_seconds()
        
        print(f"\n{'='*60}")
        print(f"Test Summary:")
        print(f"  Total requests: 10")
        print(f"  Allowed: {allowed_count}")
        print(f"  Denied: {denied_count}")
        print(f"  Total duration: {total_duration:.1f} seconds")
        print(f"  End time: {end_time.strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        
        # Basic assertions
        self.assertGreater(allowed_count, 3, "Should have allowed more than initial 3 tokens due to refill")
        self.assertGreater(denied_count, 0, "Should have denied some requests due to throttling")
        self.assertGreater(total_duration, 20, "Test should take at least 20 seconds due to throttling")
        
        print(f"\n✓ Test passed: Real-time throttling demonstrated successfully\n")


if __name__ == "__main__":
    # Run with verbose output
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRateLimiterEntity)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)