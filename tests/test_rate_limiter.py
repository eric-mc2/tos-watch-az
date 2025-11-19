"""
Direct tests for the rate_limiter_entity function.
Tests entity logic in isolation using MockEntityContext.
"""
import unittest
from datetime import datetime, timezone, timedelta
import time
from src.orchestrator import WorkflowConfig
from src.rate_limiter import rate_limiter_entity, TRY_ACQUIRE, GET_STATUS, RateLimiterState
from unittest.mock import patch, MagicMock, call


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

    config = WorkflowConfig(10, 60, 0.1, "test_processor", 2, 1)
    
    @patch("src.rate_limiter.datetime")
    def test_initial_status(self, mock_time):
        mock_time.fromisoformat = datetime.fromisoformat

        current_time = datetime.now()
        mock_time.now.return_value = current_time
        input_data = self.config.to_dict()
        context = MockEntityContext("test_workflow", GET_STATUS, input_data)
        rate_limiter_entity(context)
        
        result = context._result
        status = context.get_state()
        
        expected = RateLimiterState(self.config.rate_limit_rpm, 0, 0, current_time.isoformat())
        self.assertEqual(status, expected.to_dict())
        
        
    @patch("src.rate_limiter.datetime")
    def test_under_limit(self, mock_time):
        mock_time.fromisoformat = datetime.fromisoformat

        context = MockEntityContext("test_workflow", TRY_ACQUIRE, None)
        context._input = self.config.to_dict()
        
        n_tasks = self.config.rate_limit_rpm - 1
        tasks = [datetime(2025, 1, 1, 12, 0, i) for i in range(1,n_tasks+1)]
        
        for i, t in enumerate(tasks):
            mock_time.now.return_value = t
            rate_limiter_entity(context)
            
            result = context._result
            status = RateLimiterState.from_dict(context.get_state())
        
            self.assertTrue(result, i)
            self.assertEqual(status.remaining, self.config.rate_limit_rpm - (i + 1), i)
        
        context.operation_name = GET_STATUS
        rate_limiter_entity(context)
        status = RateLimiterState.from_dict(context.get_state())
        self.assertEqual(status.remaining, self.config.rate_limit_rpm - n_tasks)


    @patch("src.rate_limiter.datetime")
    def test_tripped(self, mock_time):
        mock_time.fromisoformat = datetime.fromisoformat

        context = MockEntityContext("test_workflow", TRY_ACQUIRE, None)
        context._input = self.config.to_dict()
        
        n_tasks = self.config.rate_limit_rpm
        times = [datetime(2025, 1, 1, 12, 0, i) for i in range(1, n_tasks+1)]
        
        for i, t in enumerate(times, 1):
            mock_time.now.return_value = t
            rate_limiter_entity(context)
            
            result = context._result
            status = RateLimiterState.from_dict(context.get_state())
        
            self.assertEqual(status.remaining, self.config.rate_limit_rpm - i, i)
            self.assertTrue(result, i)
        
        context.operation_name = GET_STATUS
        rate_limiter_entity(context)
        status = RateLimiterState.from_dict(context.get_state())
        self.assertEqual(status.remaining, 0)
        
        context.operation_name = TRY_ACQUIRE
        mock_time.now.return_value = datetime(2025, 1, 1, 12, 0, n_tasks+2)
        rate_limiter_entity(context)
        result = context._result
        status = RateLimiterState.from_dict(context.get_state())
        self.assertFalse(result)
        self.assertEqual(status.remaining, 0)

    @patch("src.rate_limiter.datetime")
    def test_reset(self, mock_time):
        # XXX: I wish I could selectively mock just the now method instead of re-enabling fromisoformat
        mock_time.fromisoformat = datetime.fromisoformat

        burst_time = datetime(2025, 1, 1, 0, 0, 0)
        second_time = burst_time + timedelta(minutes=1, seconds=1)
        third_time = burst_time + timedelta(minutes=2, seconds=2)

        burst_data = self.config.to_dict()
        context = MockEntityContext("test_workflow", TRY_ACQUIRE, burst_data)
        context._input = burst_data
        
        # Burn tokens
        mock_time.now.return_value = burst_time
        for i in range(self.config.rate_limit_rpm + 1):
            rate_limiter_entity(context)
        
        # Should be spent
        status = RateLimiterState.from_dict(context.get_state())
        self.assertFalse(context._result)
        self.assertEqual(status.remaining, 0)
        self.assertEqual(status.used_current, 10)
        self.assertEqual(status.used_previous, 0)
        
        # Next minute shifts to previous but not reset yet
        mock_time.now.return_value = second_time
        rate_limiter_entity(context)

        status = RateLimiterState.from_dict(context.get_state())
        self.assertFalse(context._result)
        self.assertEqual(status.remaining, 0)
        self.assertEqual(status.used_current, 0)
        self.assertEqual(status.used_previous, 10)
        
        # Next minute resets
        mock_time.now.return_value = third_time
        rate_limiter_entity(context)

        status = RateLimiterState.from_dict(context.get_state())
        self.assertTrue(context._result)
        self.assertEqual(status.remaining, 9)
        self.assertEqual(status.used_current, 1)
        self.assertEqual(status.used_previous, 0)


if __name__ == "__main__":
    # Run with verbose output
    suite = unittest.TestLoader().loadTestsFromTestCase(TestRateLimiterEntity)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)