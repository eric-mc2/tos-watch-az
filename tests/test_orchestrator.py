import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
import time
from azure import durable_functions as df
from src.rate_limiter import rate_limiter_entity, TRY_ACQUIRE
from src.orchestrator import orchestrator_logic, WorkflowConfig
from src.circuit_breaker import circuit_breaker_entity, GET_STATUS
from requests.exceptions import ConnectionError, ReadTimeout
from src.app_utils import pretty_error
import json

class MockDurableEntityContext:
    """Mock entity context that maintains state across calls."""
    
    def __init__(self, entity_id, state_store):
        self.entity_id = entity_id
        self.entity_key = entity_id.key
        self.operation_name = None
        self._input = None
        self._result = None
        self._state_store = state_store
        
    def get_input(self):
        return self._input
    
    def set_input(self, value):
        self._input = value
        
    def get_state(self, default_factory=None):
        key = f"{self.entity_id.name}:{self.entity_key}"
        if key not in self._state_store:
            if default_factory:
                self._state_store[key] = default_factory()
            else:
                self._state_store[key] = None
        return self._state_store[key]
    
    def set_state(self, value):
        key = f"{self.entity_id.name}:{self.entity_key}"
        self._state_store[key] = value
        
    def set_result(self, value):
        self._result = value
        
    def get_result(self):
        return self._result
    

class PrettyException(Exception):
    pass
class NestedException(Exception):
    pass
class PrettyNestedException(Exception):
    pass


class MockDurableOrchestrationContext:
    """Mock orchestration context with real entity execution."""
    
    def __init__(self, input_data, entity_state_store):
        self._input = input_data
        self.entity_state_store = entity_state_store
        self.is_replaying = False
        
        # Counters for verification
        self.success_count = 0
        self.failure_count = 0
        self.throttled_count = 0
        self.cancelled_count = 0
        
    def get_input(self):
        return self._input
    
    @property
    def current_utc_datetime(self):
        """Return actual current time."""
        return datetime.now(timezone.utc)
    
    def call_entity(self, entity_id, operation, input_data=None):
        """Execute actual entity logic."""
        # Create entity context
        entity_ctx = MockDurableEntityContext(entity_id, self.entity_state_store)
        entity_ctx.operation_name = operation
        entity_ctx.set_input(input_data)
        
        # Route to appropriate entity function
        if entity_id.name == "rate_limiter":
            rate_limiter_entity(entity_ctx)
        elif entity_id.name == "circuit_breaker":
            circuit_breaker_entity(entity_ctx)
        else:
            raise ValueError(f"Unknown entity type: {entity_id.name}")
        
        allowed = entity_ctx.get_result()
        
        # Track throttling
        if entity_id.name == "rate_limiter" and operation == TRY_ACQUIRE:
            if not allowed:
                self.throttled_count += 1
        
        # Track cancellation due to open circuit
        if entity_id.name == "circuit_breaker" and operation == GET_STATUS:
            if not allowed:
                self.cancelled_count += 1
        
        return allowed
    
    def call_activity(self, processor_name, input_data):
        # As a hack, just store the result (or error) deterministically inside the input data.
        result = input_data['result']
        if isinstance(result, Exception):
            self.failure_count += 1
            if isinstance(result, PrettyException):
                return self._wrapped_raiser(result)
            elif isinstance(result, NestedException):
                return self._nested_raiser(result)
            elif isinstance(result, PrettyNestedException):
                return self._wrapped_nested_raiser(result)
            else:
                return self._raiser(result)
        else:
            self.success_count += 1
            return result

    @pretty_error
    def _wrapped_nested_raiser(self, e, levels=3):
        self._nested_raiser(e, levels - 1)
        
    def _nested_raiser(self, e, levels=3):
        if levels:
            self._nested_raiser(e, levels - 1)
        else:
            self._raiser(e)

    @pretty_error
    def _wrapped_raiser(self, e):
        raise e
    
    def _raiser(self, e):
        raise e

    def create_timer(self, fire_at):
        """Actually sleep until the specified time."""
        now = datetime.now(timezone.utc)
        self.is_replaying = True
        if fire_at > now:
            sleep_seconds = (fire_at - now).total_seconds()
            print(f"  [WAIT] Sleeping for {sleep_seconds:.1f}s until {fire_at.strftime('%H:%M:%S')}")
            time.sleep(sleep_seconds)
        return None


def run_orchestrator(context, configs):
    """Helper to run orchestrator as a generator."""
    gen = orchestrator_logic(context, configs)
    result = None
    try:
        while True:
            yielded = gen.send(result)
            result = yielded
    except StopIteration as e:
        return e.value
    except Exception as e:
        raise


@pytest.fixture
def entity_state_store():
    """Shared state store for all entities."""
    return {}


@pytest.fixture
def rate_limit_config():
    """Config for rate limiting test."""
    return {"test_workflow": WorkflowConfig(3, 10, 10, "test_task", 1, 1)}


@pytest.fixture
def circuit_breaker_config():
    """Config for circuit breaker test."""
    return {"test_workflow": WorkflowConfig(100, 60, 5, "test_task", 1, 1)}

@pytest.fixture
def wrapper_config():
    """Config for circuit breaker test."""
    return {"test_workflow": WorkflowConfig(100, 60, 1, "test_task", 1, 1)}


@pytest.fixture
def isolation_config():
    """Config for workflow isolation test."""
    return {"workflow_a": WorkflowConfig(100, 60, 5, "process_task", 1, 1),
            "workflow_b": WorkflowConfig(100, 60, 5, "process_task", 1, 1)}


def test_rate_limiting_with_token_refill(entity_state_store, rate_limit_config):
    """
    Test that rate limiting throttles tasks and allows processing after token refill.
    
    Expected behavior:
    - First 3 tasks process immediately (consume all tokens)
    - Tasks 4-10 are throttled and wait
    - After ~60s, tokens refill and remaining tasks process
    - All tasks eventually succeed
    """
    start_time = time.time()
    
    # Submit 6 tasks
    tasks = [f"task_{i:02d}" for i in range(1, 7)]
    
    contexts = []
    
    for task_name in tasks:
        input_data = {
            "workflow_type": "test_workflow",
            "task_id": task_name,
            "result": task_name,
        }
        
        context = MockDurableOrchestrationContext(
            input_data,
            entity_state_store,
        )
        contexts.append(context)
        
        result = run_orchestrator(context, rate_limit_config)
        
    end_time = time.time()
    elapsed = end_time - start_time
    
    # Aggregate results from all contexts
    total_success = sum(ctx.success_count for ctx in contexts)
    total_failure = sum(ctx.failure_count for ctx in contexts)
    total_throttled = sum(ctx.throttled_count for ctx in contexts)
    
    # Assertions
    assert total_success == 6, f"Expected 9 successes, got {total_success}"
    assert total_failure == 0, f"Expected 0 failures, got {total_failure}"
    assert total_throttled >= 3, f"Expected at least 7 throttle events, got {total_throttled}"
    assert elapsed >= 20, f"Expected at least 20s elapsed for rate limit refill, got {elapsed:.1f}s"

def test_wrapped_error_handling(entity_state_store, wrapper_config):
    input_data = {
        "workflow_type": "test_workflow",
        "task_id": 'hello',
        "result": PrettyException('hi'),
    }
        
    context = MockDurableOrchestrationContext(
        input_data,
        entity_state_store,
    )

    with pytest.raises(Exception) as exc_info:
        result = run_orchestrator(context, wrapper_config)
    
    err = json.loads(str(exc_info.value))
    assert err['app'] == "_wrapped_raiser"
    assert err['error_type'] == PrettyException.__name__
    assert err["message"] == "hi"
    tb = err['traceback'].splitlines()
    assert "_wrapped_raiser" in tb[-1]

def test_nested_wrapped_error_handling(entity_state_store, wrapper_config):
    input_data = {
        "workflow_type": "test_workflow",
        "task_id": 'hello',
        "result": PrettyNestedException('hi'),
    }
        
    context = MockDurableOrchestrationContext(
        input_data,
        entity_state_store,
    )

    with pytest.raises(Exception) as exc_info:
        result = run_orchestrator(context, wrapper_config)
    
    err = json.loads(str(exc_info.value))
    assert err['app'] == "_wrapped_nested_raiser"
    assert err['error_type'] == PrettyNestedException.__name__
    assert err["message"] == "hi"
    tb = err['traceback'].splitlines()
    assert "_raiser" in tb[-1]

def test_unwrapped_error_handling(entity_state_store, wrapper_config):
    input_data = {
        "workflow_type": "test_workflow",
        "task_id": 'hello',
        "result": Exception('hi'),
    }
        
    context = MockDurableOrchestrationContext(
        input_data,
        entity_state_store,
    )

    with pytest.raises(Exception) as exc_info:
        result = run_orchestrator(context, wrapper_config)
    
    # This isn't a great test. It's asserting we don't mess with the error.
    assert str(exc_info.value) == "hi"

def test_unwrapped_nested_error_handling(entity_state_store, wrapper_config):
    input_data = {
        "workflow_type": "test_workflow",
        "task_id": 'hello',
        "result": NestedException('hi'),
    }
        
    context = MockDurableOrchestrationContext(
        input_data,
        entity_state_store,
    )

    with pytest.raises(Exception) as exc_info:
        result = run_orchestrator(context, wrapper_config)
    
    # This isn't a great test. It's asserting we don't mess with the error.
    assert str(exc_info.value) == "hi"


def test_circuit_breaker_trips_and_stops_processing(entity_state_store, circuit_breaker_config):
    """
    Test that circuit breaker trips on non-retryable error and stops subsequent tasks.
    
    Expected behavior:
    - Tasks 1-2 succeed
    - Task 3 fails with non-retryable error and trips circuit
    - Tasks 4-8 immediately fail due to open circuit (no activity calls)
    """    
    # Submit 10 tasks
    tasks = [f"task_{i:02d}" for i in range(1, 11)]
    results = [f"task_{i:02d}" for i in range(1, 11)]
    results[2] = Exception("Test fail")
    results[3] = Exception("Test fail")
    results[4] = Exception("Test fail")
    
    contexts = []
    
    for i, (task_name, result) in enumerate((zip(tasks, results)), 1):
        input_data = {
            "workflow_type": "test_workflow",
            "task_id": task_name,
            "result": result,
        }
        
        context = MockDurableOrchestrationContext(
            input_data,
            entity_state_store,
        )
        contexts.append(context)
                
        try:
            result = run_orchestrator(context, circuit_breaker_config)
        except Exception as e:
            print(f"Task {task_name} failed with {e.__class__.__name__}")
    
    # Aggregate results from all contexts
    total_success = sum(ctx.success_count for ctx in contexts)
    total_failure = sum(ctx.failure_count for ctx in contexts)
    total_cancelled = sum(ctx.cancelled_count for ctx in contexts)
    
    # Assertions
    assert total_success == 2, f"Expected 2 successes (tasks 1-2), got {total_success}"
    assert total_failure == 3, f"Expected 3 failure (tasks 3,4,5), got {total_failure}"
    assert total_cancelled == 5, f"Expected 5 cancelled (tasks 6-10), got {total_cancelled}"

def test_workflow_isolation_separate_circuits(entity_state_store, isolation_config):
    """
    Test that different workflow types have isolated rate limiters and circuit breakers.
    
    Expected behavior:
    - workflow_a task 2 fails and trips circuit_a
    - workflow_b tasks continue processing normally (circuit_b unaffected)
    - workflow_a task 3 blocked by open circuit
    """
    # Interleave tasks from both workflows
    task_sequence = [
        ("workflow_a", "task_a1", "success"),
        ("workflow_b", "task_b1", "success"),
        ("workflow_a", "task_a2", Exception('Fail')),  # This will fail and trip circuit_a
        ("workflow_a", "task_a3", Exception('Fail')),  # This will fail and trip circuit_a
        ("workflow_a", "task_a4", Exception('Fail')),  # This will fail and trip circuit_a
        ("workflow_b", "task_b2", "success?"),
        ("workflow_b", "task_b3", "success?"),
        ("workflow_a", "task_a5", "cancel?"),  # This should be blocked
    ]
    
    contexts_a = []
    contexts_b = []
    
    for workflow_type, task_name, result in task_sequence:
        input_data = {
            "workflow_type": workflow_type,
            "task_id": task_name,
            "result": result,
        }
        
        context = MockDurableOrchestrationContext(
            input_data,
            entity_state_store,
        )
        
        if workflow_type == "workflow_a":
            contexts_a.append(context)
        else:
            contexts_b.append(context)
        
        try:
            result = run_orchestrator(context, isolation_config)
        except Exception as e:
            print(f"Task {task_name} failed with {e.__class__.__name__}")
            
    # Aggregate results per workflow
    results_a = {
        "success": sum(ctx.success_count for ctx in contexts_a),
        "failure": sum(ctx.failure_count for ctx in contexts_a),
        "cancelled": sum(ctx.cancelled_count for ctx in contexts_a)
    }
    results_b = {
        "success": sum(ctx.success_count for ctx in contexts_b),
        "failure": sum(ctx.failure_count for ctx in contexts_b),
        "cancelled": sum(ctx.cancelled_count for ctx in contexts_b)
    }
    
    assert results_a["success"] == 1, f"Expected 1 success for workflow_a (task_a1), got {results_a['success']}"
    assert results_a["failure"] == 3, f"Expected 3 failure for workflow_a (task_a2), got {results_a['failure']}"
    assert results_a["cancelled"] == 1, f"Expected 1 cancelled for workflow_a (task_a3), got {results_a['cancelled']}"
    assert results_b["success"] == 3, f"Expected 3 successes for workflow_b, got {results_b['success']}"
    assert results_b["failure"] == 0, f"Expected 0 failures for workflow_b, got {results_b['failure']}"
    assert results_b["cancelled"] == 0, f"Expected 0 cancelled for workflow_b, got {results_b['cancelled']}"
    