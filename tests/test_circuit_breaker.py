"""
Direct tests for the circuit_breaker_entity function.
Tests entity logic in isolation using MockEntityContext.
"""
import unittest
from datetime import datetime, timezone
from src.circuit_breaker import circuit_breaker_entity, GET_STATUS, RESET, TRIP

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
    
    def test_get_status_when_closed(self):
        """Test that get_status returns correct state when circuit is closed"""
        state = self.initial_state.copy()
        
        context = MockEntityContext("test_workflow", GET_STATUS, None)
        context.set_state(state)
        circuit_breaker_entity(context)
        
        allowed = context._result
        status = context.get_state()
        
        self.assertTrue(allowed, "Status should be returned")
        self.assertFalse(status['is_open'], "Circuit should be closed")
        self.assertIsNone(status['error_message'], "No error message when closed")
        self.assertEqual(status['strikes'], 3, "Default strikes")
            
    def test_trip_opens_circuit(self):
        """Test that trip operation opens the circuit"""
        state = self.initial_state.copy()
        state['strikes'] = 0
                
        error_msg = "Test error"
        context = MockEntityContext("test_workflow", TRIP, error_msg)
        context.set_state(state)
        circuit_breaker_entity(context)
        
        state = context.get_state()
        allowed = context._result
        
        self.assertFalse(allowed, "Trip operation should return True")
        self.assertTrue(state['is_open'], "Circuit should be open after trip")
        self.assertEqual(state['strikes'], 0, "No strikes")
        self.assertEqual(state['error_message'], error_msg, "Error message should be stored")
        self.assertIsNotNone(state['opened_at'], "opened_at timestamp should be set")
            
    def test_get_status_when_open(self):
        """Test that get_status returns correct state when circuit is open"""
        
        # Start with open circuit
        error_msg = "Test error"
        error_time = datetime.now(timezone.utc).isoformat()
        state = {
            "strikes": 0,
            "is_open": True,
            "error_message": error_msg,
            "opened_at": error_time
        }
        
        context = MockEntityContext("test_workflow", GET_STATUS, None)
        context.set_state(state)
        circuit_breaker_entity(context)
        
        allowed = context._result
        status = context.get_state()
        
        self.assertFalse(allowed, "Status should be returned")
        self.assertTrue(status['is_open'], "Status should show circuit is open")
        self.assertEqual(status['strikes'], 0, "No more strikes")
        self.assertEqual(status['error_message'], error_msg, "Error message should be included")
        self.assertEqual(status['opened_at'], error_time, "Set error time")
        
        print(f"\n✓ Test passed: Status correctly shows open circuit\n")
    
    def test_reset_closes_circuit(self):
        """Test the reset operation closes the circuit"""
        
        # Start with open circuit
        state = {
            "strikes": 0,
            "is_open": True,
            "error_message": "FATAL: Previous error",
            "opened_at": datetime.now(timezone.utc).isoformat()
        }
        
        print(f"  Initial state: OPEN")
        print(f"  Error: {state['error_message']}")
        
        context = MockEntityContext("test_workflow", RESET, None)
        context.set_state(state)
        circuit_breaker_entity(context)
        
        state = context.get_state()
        allowed = context._result
        
        self.assertTrue(allowed, "Reset operation should return True")
        self.assertFalse(state['is_open'], "Circuit should be closed after reset")
        self.assertEqual(state['strikes'], 3, "Reset strikes")
        self.assertIsNone(state['error_message'], "Error message should be cleared")
        self.assertIsNone(state['opened_at'], "opened_at should be cleared")
            
    def test_multiple_trips(self):
        """Test that multiple trips update the error message"""
        
        state = self.initial_state.copy()
        
        errors = [
            "FATAL: Database connection failed",
            "FATAL: Schema mismatch",
            "FATAL: Critical system error"
        ]
        
        for i, error in enumerate(errors, 1):
            context = MockEntityContext("test_workflow", TRIP, error)
            context.set_state(state)
            circuit_breaker_entity(context)
            
            state = context.get_state()
            allowed = context._result
            
            self.assertEqual(allowed, i < 3, "3rd strike still passes")
            self.assertEqual(state['is_open'], i == 3, "Still closed after third strike")
            self.assertEqual(state['strikes'], 3 - i, "No more strikes")
            if i < 3:
                self.assertIsNone(state['error_message'], "Error message not yet")
                self.assertIsNone(state['opened_at'], "opened_at not yet")
            else:
                self.assertIsNotNone(state['error_message'], "Error message now")
                self.assertIsNotNone(state['opened_at'], "opened_at now")
                
    def test_state_initialization_on_first_call(self):
        """Test that entity initializes state correctly on first call"""
        
        # No pre-existing state
        context = MockEntityContext("test_workflow", GET_STATUS, None)
        # Don't set context._state, let entity initialize it
        
        circuit_breaker_entity(context)
        state = context.get_state()
        allowed = context._result
                
        self.assertTrue(allowed, "Allowed by default")
        self.assertIsNotNone(state, "State should be initialized")
        self.assertFalse(state['is_open'], "Should initialize as closed")
        self.assertIsNone(state['error_message'], "Should have no error initially")
        self.assertIsNone(state['opened_at'], "Should have no opened_at initially")
