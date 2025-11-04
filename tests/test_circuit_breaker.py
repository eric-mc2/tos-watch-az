"""
Direct tests for the circuit_breaker_entity function.
Tests entity logic in isolation using MockEntityContext.
"""
import unittest
from datetime import datetime, timezone
from src.circuit_breaker import circuit_breaker_entity

class MockEntityContext:
    """Mock DurableEntityContext for testing the circuit breaker entity"""
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


class TestCircuitBreakerEntity(unittest.TestCase):
    """Test the circuit breaker entity directly"""
    
    def setUp(self):
        """Set up initial state"""
        self.initial_state = {
            "strikes": 3,
            "is_open": False,
            "error_message": None,
            "opened_at": None
        }
    
    async def test_get_status_when_closed(self):
        """Test that get_status returns correct state when circuit is closed"""
        print("\n" + "="*60)
        print("TEST: Get Status When Closed")
        print("="*60)
                
        state = self.initial_state.copy()
        
        context = MockEntityContext("test_workflow", "get_status", None)
        context._state = state
        await circuit_breaker_entity(context)
        
        result = context._result
        
        print(f"  Circuit state: CLOSED")
        print(f"  get_status() result: {result}")
        
        self.assertIsNotNone(result, "Status should be returned")
        self.assertFalse(result['is_open'], "Circuit should be closed")
        self.assertIsNone(result['error_message'], "No error message when closed")
        
        print(f"\n✓ Test passed: Status correctly shows closed circuit\n")
    
    async def test_trip_opens_circuit(self):
        """Test that trip operation opens the circuit"""
        print("\n" + "="*60)
        print("TEST: Trip Opens Circuit")
        print("="*60)
                
        state = self.initial_state.copy()
        
        # Trip the circuit with an error message
        error_msg = "FATAL: Database schema mismatch"
        
        print(f"  Initial state: CLOSED")
        print(f"  Tripping circuit with error: {error_msg}")
        
        context = MockEntityContext("test_workflow", "trip", error_msg)
        context._state = state
        await circuit_breaker_entity(context)
        
        state = context.get_state()
        result = context._result
        
        print(f"  New state: {'OPEN' if state['is_open'] else 'CLOSED'}")
        print(f"  Error message: {state['error_message']}")
        print(f"  Trip result: {result}")
        
        self.assertTrue(result, "Trip operation should return True")
        self.assertTrue(state['is_open'], "Circuit should be open after trip")
        self.assertEqual(state['error_message'], error_msg, "Error message should be stored")
        self.assertIsNotNone(state['opened_at'], "opened_at timestamp should be set")
        
        print(f"\n✓ Test passed: Circuit opened on trip\n")
    
    async def test_get_status_when_open(self):
        """Test that get_status returns correct state when circuit is open"""
        print("\n" + "="*60)
        print("TEST: Get Status When Open")
        print("="*60)
                
        # Start with open circuit
        error_msg = "FATAL: Systemic failure detected"
        state = {
            "strikes": 0,
            "is_open": True,
            "error_message": error_msg,
            "opened_at": datetime.now(timezone.utc).isoformat()
        }
        
        print(f"  Circuit state: OPEN")
        print(f"  Error: {state['error_message']}")
        
        context = MockEntityContext("test_workflow", "get_status", None)
        context._state = state
        await circuit_breaker_entity(context)
        
        result = context._result
        
        print(f"  get_status() result: {result}")
        
        self.assertIsNotNone(result, "Status should be returned")
        self.assertTrue(result['is_open'], "Status should show circuit is open")
        self.assertEqual(result['error_message'], error_msg, "Error message should be included")
        
        print(f"\n✓ Test passed: Status correctly shows open circuit\n")
    
    async def test_reset_closes_circuit(self):
        """Test the reset operation closes the circuit"""
        print("\n" + "="*60)
        print("TEST: Reset Closes Circuit")
        print("="*60)
                
        # Start with open circuit
        state = {
            "strikes": 0,
            "is_open": True,
            "error_message": "FATAL: Previous error",
            "opened_at": datetime.now(timezone.utc).isoformat()
        }
        
        print(f"  Initial state: OPEN")
        print(f"  Error: {state['error_message']}")
        
        context = MockEntityContext("test_workflow", "reset", None)
        context._state = state
        await circuit_breaker_entity(context)
        
        state = context.get_state()
        result = context._result
        
        print(f"  After reset: {'OPEN' if state['is_open'] else 'CLOSED'}")
        print(f"  Reset result: {result}")
        
        self.assertTrue(result, "Reset operation should return True")
        self.assertFalse(state['is_open'], "Circuit should be closed after reset")
        self.assertIsNone(state['error_message'], "Error message should be cleared")
        self.assertIsNone(state['opened_at'], "opened_at should be cleared")
        
        print(f"\n✓ Test passed: Reset operation works correctly\n")
    
    async def test_multiple_trips(self):
        """Test that multiple trips update the error message"""
        print("\n" + "="*60)
        print("TEST: Multiple Trips")
        print("="*60)
                
        state = self.initial_state.copy()
        
        errors = [
            "FATAL: Database connection failed",
            "FATAL: Schema mismatch",
            "FATAL: Critical system error"
        ]
        
        for i, error in enumerate(errors, 1):
            print(f"\n  Trip #{i}: {error}")
            
            context = MockEntityContext("test_workflow", "trip", error)
            context._state = state
            await circuit_breaker_entity(context)
            
            state = context.get_state()
            
            self.assertTrue(state['is_open'], f"Circuit should be open after trip #{i}")
            self.assertEqual(state['error_message'], error, f"Error message should be updated on trip #{i}")
            
            print(f"    State: OPEN, Message: {state['error_message']}")
        
        print(f"\n✓ Test passed: Multiple trips handled correctly\n")
    
    async def test_is_retryable_error_function(self):
        """Test the is_retryable_error helper function"""
        print("\n" + "="*60)
        print("TEST: is_retryable_error Helper Function")
        print("="*60)
        
        from src.rate_limiter import is_retryable_error
        
        test_cases = [
            ("Max retries exceeded with url: Connection refused", True),
            ("Connection reset by peer", True),
            ("Timeout waiting for response", True),
            ("Too Many Requests", True),
            ("HTTP 429 error", True),
            ("Service unavailable 503", True),
            ("Gateway timeout 504", True),
            ("Connection pool is full", True),
            ("FATAL: Database schema error", False),
            ("FATAL: Invalid configuration", False),
            ("Some random error", False),
            ("Unexpected exception occurred", False),
        ]
        
        for error_msg, expected in test_cases:
            result = is_retryable_error(error_msg)
            status = "✓" if result == expected else "✗"
            print(f"  {status} '{error_msg[:50]}...' -> retryable={result} (expected={expected})")
            self.assertEqual(result, expected, f"Incorrect classification for: {error_msg}")
        
        print(f"\n✓ Test passed: is_retryable_error works correctly\n")
    
    async def test_state_initialization_on_first_call(self):
        """Test that entity initializes state correctly on first call"""
        print("\n" + "="*60)
        print("TEST: State Initialization")
        print("="*60)
                
        # No pre-existing state
        context = MockEntityContext("test_workflow", "get_status", None)
        # Don't set context._state, let entity initialize it
        
        await circuit_breaker_entity(context)
        
        state = context.get_state()
        result = context._result
        
        print(f"  Initialized state: {state}")
        
        self.assertIsNotNone(state, "State should be initialized")
        self.assertFalse(state['is_open'], "Should initialize as closed")
        self.assertIsNone(state['error_message'], "Should have no error initially")
        self.assertIsNone(state['opened_at'], "Should have no opened_at initially")
        
        print(f"\n✓ Test passed: State initialization works correctly\n")


if __name__ == "__main__":
    # Run with verbose output
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCircuitBreakerEntity)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)