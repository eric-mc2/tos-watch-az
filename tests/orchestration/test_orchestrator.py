import pytest
from datetime import datetime, timezone
import time
from azure import durable_functions as df  # type: ignore
from src.orchestration.rate_limiter import rate_limiter_entity, TRY_ACQUIRE
from src.orchestration.orchestrator import orchestrator_logic, WorkflowConfig
from src.orchestration.circuit_breaker import circuit_breaker_entity, GET_STATUS
from src.utils.app_utils import pretty_error
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
        
        # Event handling for orchestrator suspension/resumption
        self._waiting_for_event = None
        self._pending_events = {}
        
    def get_input(self):
        return self._input
    
    @property
    def current_utc_datetime(self):
        """Return actual current time."""
        return datetime.now(timezone.utc)
    
    def set_custom_status(self, *args, **kwargs):
        pass
    
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
    
    def wait_for_external_event(self, event_name):
        """Mark orchestrator as waiting for an event - this suspends execution."""
        self._waiting_for_event = event_name
        # Return the event data if it's already been raised, otherwise None
        return self._pending_events.get(event_name)
    
    def raise_event(self, event_name, data=None):
        """Raise an event to wake waiting orchestrators."""
        self._pending_events[event_name] = data
        if self._waiting_for_event == event_name:
            self._waiting_for_event = None
    
    def continue_as_new(self, input_data):
        """Restart orchestrator with new input."""
        raise StopIteration("continue_as_new")

def run_orchestrator(context, configs, gen = None):
    """Helper to run orchestrator as a generator until completion or suspension.
    Gen argument resumes a suspended orchestrator generator after an event is raised.
    The test environment is not threaded so this is a simulation of the generator awaiting events.
    """
    if gen is None:
        gen = orchestrator_logic(context, configs)
    # Send the event data to the waiting orchestrator
    event_data = context._pending_events.get(context._waiting_for_event)
    result = event_data
    
    try:
        while True:
            yielded = gen.send(result)
            result = yielded
            
            # Check if orchestrator suspended again
            if context._waiting_for_event is not None:
                return ('suspended', gen)
                
    except StopIteration as e:
        return ('completed', e.value)
    except RuntimeError as e:
        if 'continue_as_new' in str(e) or "StopIteration" in str(e):
            return ('completed', None)
        else:
            raise
    except Exception as e:
        raise

@pytest.fixture
def entity_state_store():
    """Shared state store for all entities."""
    return {}


@pytest.fixture
def rate_limit_config():
    """Config for rate limiting test."""
    return {"test_workflow": WorkflowConfig(3, 2, 4, "test_task", 1, 1)}


@pytest.fixture
def circuit_breaker_config():
    """Config for circuit breaker test."""
    return {"test_workflow": WorkflowConfig(1000, 60, 5, "test_task", 1, 1)}

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
    NOTE THIS TEST IS SINGLE THREADED SO EXPECTATIONS ARE DIFFERENT THAN SUT
    TO SIMPLIFY THE FRACTIONAL WINDOW MATH, CONFIG SLEEPS FOR DOUBLE THE RATE PERIOD
    - First 3 tasks process immediately (consume all tokens)
    - 4th task is throttled
    - Second window elapses
    - Tasks 4-6 process in third window
    - 7th task is throttled
    - Third and fourth windows elapse
    - Tasks 7-9 process in the fifth window.
    In total,
    - All tasks succeed
    - Five periods elapse
    - Two tasks are throttled
    """
    start_time = time.time()
    
    # Submit 9 tasks
    tasks = [f"task_{i:02d}" for i in range(9)]
    
    contexts = []
    for task_name in tasks:
        input_data = {"workflow_type": "test_workflow", "task_id": task_name, "result": task_name}
        context = MockDurableOrchestrationContext(input_data, entity_state_store)
        contexts.append(context)
        result = run_orchestrator(context, rate_limit_config)
        
    end_time = time.time()
    elapsed = end_time - start_time
    
    # Aggregate results from all contexts
    total_success = sum(ctx.success_count for ctx in contexts)
    total_failure = sum(ctx.failure_count for ctx in contexts)
    total_throttled = sum(ctx.throttled_count for ctx in contexts)
    
    # Assertions
    assert total_success == 9, f"Expected 9 successes, got {total_success}"
    assert total_failure == 0, f"Expected 0 failures, got {total_failure}"
    assert total_throttled == 2, f"Expected at 2 throttle events, got {total_throttled}"
    period = rate_limit_config['test_workflow'].rate_limit_period
    assert elapsed >= period * 4, f"Expected at least 8s elapsed for rate limit refill, got {elapsed:.1f}s"
    assert elapsed <= period * 5, f"Expected at least 10s elapsed for rate limit refill, got {elapsed:.1f}s"

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
    tb = err['traceback']
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
    tb = err['traceback']
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

def test_tasks_resume_after_circuit_resets(entity_state_store, circuit_breaker_config):
    """
    Test that cancelled tasks wake when circuit resets.
    
    Expected behavior:
    - Breaker starts open
    - Tasks are blocked 
    - Breaker resets
    - Tasks resume
    """
    from src.orchestration.circuit_breaker import GET_STATUS, RESET, circuit_breaker_entity
    
    # Initialize open circuit by tripping it with failures
    workflow_type = "test_workflow"
    circuit_entity_id = df.EntityId("circuit_breaker", workflow_type)
    
    for i in range(3):
        input_data = {"workflow_type": workflow_type, "task_id": f"task_{i}", "result": Exception(f"strike_{i}")}
        context = MockDurableOrchestrationContext(input_data, entity_state_store)
        with(pytest.raises(Exception)):
            run_orchestrator(context, circuit_breaker_config)
    
    # Verify circuit is open
    check_ctx = MockDurableEntityContext(circuit_entity_id, entity_state_store)
    check_ctx.operation_name = GET_STATUS
    circuit_breaker_entity(check_ctx)
    assert check_ctx.get_result() == False, "Circuit should be open"
    
    # Send in tasks that will block on the circuit
    tasks = [f"task_{i:02d}" for i in range(10)]
    results = [f"task_{i:02d}" for i in range(10)]    
    suspended_orchestrators = []
    
    for i, (task_name, result) in enumerate(zip(tasks, results)):
        input_data = {"workflow_type": workflow_type, "task_id": task_name, "result": result}
        context = MockDurableOrchestrationContext(input_data, entity_state_store)
        
        # Start orchestrator - it should suspend on wait_for_external_event
        status, gen_or_value = run_orchestrator(context, circuit_breaker_config)
        
        assert status == 'suspended', f"Task {task_name} should be suspended, got {status}"
        assert context._waiting_for_event == RESET, f"Task should be waiting for RESET event"
        
        suspended_orchestrators.append((context, gen_or_value))

    # Verify tasks are pending (blocked on circuit)
    total_success = sum(ctx.success_count for ctx, _ in suspended_orchestrators)
    total_failure = sum(ctx.failure_count for ctx, _ in suspended_orchestrators)
    total_cancelled = sum(ctx.cancelled_count for ctx, _ in suspended_orchestrators)
    
    assert total_success == 0, "No tasks should succeed yet"
    assert total_failure == 0, "No tasks should fail"
    assert total_cancelled == 10, "All tasks should be cancelled by circuit"

    # Reset circuit
    reset_ctx = MockDurableEntityContext(circuit_entity_id, entity_state_store)
    reset_ctx.operation_name = RESET
    circuit_breaker_entity(reset_ctx)
    assert reset_ctx.get_result() == True, "Circuit should reset successfully"
    
    # Verify circuit is closed
    check_ctx2 = MockDurableEntityContext(circuit_entity_id, entity_state_store)
    check_ctx2.operation_name = GET_STATUS
    circuit_breaker_entity(check_ctx2)
    assert check_ctx2.get_result() == True, "Circuit should be closed"
    
    # Raise RESET event to wake all waiting orchestrators and resume them
    for i, (context, gen) in enumerate(suspended_orchestrators):
        context.raise_event(RESET)
        
        # Resume the suspended orchestrator - it should restart and complete
        status, value = run_orchestrator(context, circuit_breaker_config, gen)
        assert status == 'completed' 

        # It's not done-done. We have to simulate continue_as_new.
        # XXX: This is kinda messed up but since the test simultaes continue_as_new
        # via a StopIteration error, that actually raises inside the orchestrator's
        # error handling logic and causes the orchestrator to trip the circuit again.
        # So FOR THIS TEST ONLY, we re-raise the circuit to keep it closed.
        circuit_breaker_entity(reset_ctx)

        status, value = run_orchestrator(context, circuit_breaker_config)
        
        assert status == 'completed', f"Task should complete after reset, got {status}"

    # Verify tasks have now completed
    total_success = sum(ctx.success_count for ctx, _ in suspended_orchestrators)
    total_failure = sum(ctx.failure_count for ctx, _ in suspended_orchestrators)
    total_cancelled = sum(ctx.cancelled_count for ctx, _ in suspended_orchestrators)
    
    assert total_success == 10, f"All tasks should succeed after reset, got {total_success}"
    assert total_failure == 0, f"No tasks should fail, got {total_failure}"
    assert total_cancelled == 10, f"Cancelled count should remain at 10, got {total_cancelled}"


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
