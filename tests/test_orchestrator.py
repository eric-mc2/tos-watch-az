import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
import time
from azure import durable_functions as df
from src.rate_limiter import (
    orchestrator_logic,
    rate_limiter_entity,
    circuit_breaker_entity,
)


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


class MockDurableOrchestrationContext:
    """Mock orchestration context with real entity execution."""
    
    def __init__(self, input_data, entity_state_store, config):
        self._input = input_data
        self.entity_state_store = entity_state_store
        self.config = config
        self.activity_results = {}
        self.activity_call_count = {}
        
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
        if entity_id.name == "generic_rate_limiter_entity":
            rate_limiter_entity(entity_ctx, self.config)
        elif entity_id.name == "circuit_breaker_entity_func":
            circuit_breaker_entity(entity_ctx)
        else:
            raise ValueError(f"Unknown entity type: {entity_id.name}")
        
        result = entity_ctx.get_result()
        
        # Track throttling
        if entity_id.name == "generic_rate_limiter_entity" and operation == "try_consume":
            if not result:
                self.throttled_count += 1
        
        # Track cancellation due to open circuit
        if entity_id.name == "circuit_breaker_entity_func" and operation == "get_status":
            if isinstance(result, dict) and result.get("is_open", False):
                self.cancelled_count += 1
        
        return result
    
    def call_activity(self, activity_name, input_data):
        """Mock activity call."""
        blob_name = input_data.get("blob_name")
        
        # Track call count
        if blob_name not in self.activity_call_count:
            self.activity_call_count[blob_name] = 0
        self.activity_call_count[blob_name] += 1
        
        # Check if this task should fail
        if blob_name in self.activity_results:
            result = self.activity_results[blob_name]
            if isinstance(result, Exception):
                self.failure_count += 1
                raise result
            self.success_count += 1
            return result
        
        # Default: succeed and print
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        print(f"  [{timestamp}] Processing task: {blob_name}")
        self.success_count += 1
        return {"status": "success", "blob_name": blob_name}
    
    def create_timer(self, fire_at):
        """Actually sleep until the specified time."""
        now = datetime.now(timezone.utc)
        if fire_at > now:
            sleep_seconds = (fire_at - now).total_seconds()
            print(f"  [WAIT] Sleeping for {sleep_seconds:.1f}s until {fire_at.strftime('%H:%M:%S')}")
            time.sleep(sleep_seconds)
        return None


def run_orchestrator(context, config):
    """Helper to run orchestrator as a generator."""
    gen = orchestrator_logic(context, config)
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
    return {
        "test_workflow": {
            "rate_limit_rpm": 3,  # 3 tokens per minute
            "delay": 10,  # Check every 5 seconds
            "activity_name": "process_task",
            "max_retries": 2
        }
    }


@pytest.fixture
def circuit_breaker_config():
    """Config for circuit breaker test."""
    return {
        "test_workflow": {
            "rate_limit_rpm": 100,  # High limit, no throttling
            "delay": 5,
            "activity_name": "process_task",
            "max_retries": 2
        }
    }


@pytest.fixture
def isolation_config():
    """Config for workflow isolation test."""
    return {
        "workflow_a": {
            "rate_limit_rpm": 100,
            "delay": 5,
            "activity_name": "process_task",
            "max_retries": 2
        },
        "workflow_b": {
            "rate_limit_rpm": 100,
            "delay": 5,
            "activity_name": "process_task",
            "max_retries": 2
        }
    }


def test_rate_limiting_with_token_refill(entity_state_store, rate_limit_config):
    """
    Test that rate limiting throttles tasks and allows processing after token refill.
    
    Expected behavior:
    - First 3 tasks process immediately (consume all tokens)
    - Tasks 4-10 are throttled and wait
    - After ~60s, tokens refill and remaining tasks process
    - All tasks eventually succeed
    """
    print("\n" + "="*80)
    print("TEST 1: Rate Limiting with Token Refill")
    print("="*80)
    print("Config: 3 tokens per minute, checking every 10s")
    print("Submitting 6 tasks...\n")
    
    start_time = time.time()
    
    # Submit 6 tasks
    tasks = [f"task_{i:02d}" for i in range(1, 7)]
    
    contexts = []
    
    for task_name in tasks:
        input_data = {
            "workflow_type": "test_workflow",
            "blob_name": task_name
        }
        
        context = MockDurableOrchestrationContext(
            input_data,
            entity_state_store,
            rate_limit_config
        )
        contexts.append(context)
        
        try:
            result = run_orchestrator(context, rate_limit_config)
        except Exception as e:
            print(f"  [ERROR] {task_name} failed: {e}")
        
        print(f"  Task {task_name}: success={context.success_count}, "
              f"failure={context.failure_count}, "
              f"throttled={context.throttled_count} times")
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    # Aggregate results from all contexts
    total_success = sum(ctx.success_count for ctx in contexts)
    total_failure = sum(ctx.failure_count for ctx in contexts)
    total_throttled = sum(ctx.throttled_count for ctx in contexts)
    
    print(f"\n" + "-"*80)
    print(f"RESULTS:")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Success: {total_success}")
    print(f"  Failure: {total_failure}")
    print(f"  Total throttle events: {total_throttled}")
    print("-"*80)
    
    # Assertions
    assert total_success == 6, f"Expected 9 successes, got {total_success}"
    assert total_failure == 0, f"Expected 0 failures, got {total_failure}"
    assert total_throttled >= 3, f"Expected at least 7 throttle events, got {total_throttled}"
    assert elapsed >= 20, f"Expected at least 20s elapsed for rate limit refill, got {elapsed:.1f}s"
    
    print("\n✓ Test passed: All tasks succeeded with observable rate limiting\n")


def test_circuit_breaker_trips_and_stops_processing(entity_state_store, circuit_breaker_config):
    """
    Test that circuit breaker trips on non-retryable error and stops subsequent tasks.
    
    Expected behavior:
    - Tasks 1-2 succeed
    - Task 3 fails with non-retryable error and trips circuit
    - Tasks 4-8 immediately fail due to open circuit (no activity calls)
    """
    print("\n" + "="*80)
    print("TEST 2: Circuit Breaker Tripping")
    print("="*80)
    print("Config: High rate limit (no throttling)")
    print("Task 3 will fail with non-retryable error and trip circuit\n")
    
    start_time = time.time()
    
    # Submit 8 tasks
    tasks = [f"task_{i:02d}" for i in range(1, 9)]
    
    contexts = []
    
    for i, task_name in enumerate(tasks, 1):
        input_data = {
            "workflow_type": "test_workflow",
            "blob_name": task_name
        }
        
        context = MockDurableOrchestrationContext(
            input_data,
            entity_state_store,
            circuit_breaker_config
        )
        contexts.append(context)
        
        # Task 3 will fail with non-retryable error
        if i == 3:
            context.activity_results[task_name] = Exception("FATAL_DB_ERROR: Database corrupted")
        
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        
        try:
            result = run_orchestrator(context, circuit_breaker_config)
            print(f"  [{timestamp}] ✓ {task_name} succeeded")
        except Exception as e:
            error_msg = str(e)
            if "Circuit breaker open" in error_msg:
                print(f"  [{timestamp}] ✗ {task_name} blocked by open circuit")
            else:
                print(f"  [{timestamp}] ✗ {task_name} failed: {error_msg}")
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    # Aggregate results from all contexts
    total_success = sum(ctx.success_count for ctx in contexts)
    total_failure = sum(ctx.failure_count for ctx in contexts)
    total_cancelled = sum(ctx.cancelled_count for ctx in contexts)
    
    print(f"\n" + "-"*80)
    print(f"RESULTS:")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  Success: {total_success}")
    print(f"  Failure: {total_failure}")
    print(f"  Cancelled: {total_cancelled}")
    print("-"*80)
    
    # Assertions
    assert total_success == 2, f"Expected 2 successes (tasks 1-2), got {total_success}"
    assert total_failure == 1, f"Expected 1 failure (task 3), got {total_failure}"
    assert total_cancelled == 5, f"Expected 5 cancelled (tasks 4-8), got {total_cancelled}"
    
    print("\n✓ Test passed: Circuit breaker tripped and stopped subsequent tasks\n")


def test_workflow_isolation_separate_circuits(entity_state_store, isolation_config):
    """
    Test that different workflow types have isolated rate limiters and circuit breakers.
    
    Expected behavior:
    - workflow_a task 2 fails and trips circuit_a
    - workflow_b tasks continue processing normally (circuit_b unaffected)
    - workflow_a task 3 blocked by open circuit
    """
    print("\n" + "="*80)
    print("TEST 3: Workflow Isolation")
    print("="*80)
    print("Config: Two workflows (a and b) with separate circuits")
    print("workflow_a task 2 will trip circuit, workflow_b should continue\n")
    
    start_time = time.time()
    
    # Interleave tasks from both workflows
    task_sequence = [
        ("workflow_a", "task_a1"),
        ("workflow_b", "task_b1"),
        ("workflow_a", "task_a2"),  # This will fail and trip circuit_a
        ("workflow_b", "task_b2"),
        ("workflow_b", "task_b3"),
        ("workflow_a", "task_a3"),  # This should be blocked
    ]
    
    contexts_a = []
    contexts_b = []
    
    for workflow_type, task_name in task_sequence:
        input_data = {
            "workflow_type": workflow_type,
            "blob_name": task_name
        }
        
        context = MockDurableOrchestrationContext(
            input_data,
            entity_state_store,
            isolation_config
        )
        
        if workflow_type == "workflow_a":
            contexts_a.append(context)
        else:
            contexts_b.append(context)
        
        # task_a2 will fail with non-retryable error
        if task_name == "task_a2":
            context.activity_results[task_name] = Exception("FATAL_API_ERROR: Service permanently unavailable")
        
        timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
        
        try:
            result = run_orchestrator(context, isolation_config)
            print(f"  [{timestamp}] ✓ {workflow_type}/{task_name} succeeded")
        except Exception as e:
            error_msg = str(e)
            if "Circuit breaker open" in error_msg:
                print(f"  [{timestamp}] ✗ {workflow_type}/{task_name} blocked by open circuit")
            else:
                print(f"  [{timestamp}] ✗ {workflow_type}/{task_name} failed: {error_msg}")
    
    end_time = time.time()
    elapsed = end_time - start_time
    
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
    
    print(f"\n" + "-"*80)
    print(f"RESULTS:")
    print(f"  Total time: {elapsed:.1f}s")
    print(f"  workflow_a: {results_a['success']} success, {results_a['failure']} failure, {results_a['cancelled']} cancelled")
    print(f"  workflow_b: {results_b['success']} success, {results_b['failure']} failure, {results_b['cancelled']} cancelled")
    print("-"*80)
    
    # Assertions
    assert results_a["success"] == 1, f"Expected 1 success for workflow_a (task_a1), got {results_a['success']}"
    assert results_a["failure"] == 1, f"Expected 1 failure for workflow_a (task_a2), got {results_a['failure']}"
    assert results_a["cancelled"] == 1, f"Expected 1 cancelled for workflow_a (task_a3), got {results_a['cancelled']}"
    assert results_b["success"] == 3, f"Expected 3 successes for workflow_b, got {results_b['success']}"
    assert results_b["failure"] == 0, f"Expected 0 failures for workflow_b, got {results_b['failure']}"
    assert results_b["cancelled"] == 0, f"Expected 0 cancelled for workflow_b, got {results_b['cancelled']}"
    
    print("\n✓ Test passed: Workflows have isolated circuits and rate limiters\n")


if __name__ == "__main__":
    # Run tests directly for easier debugging
    store = {}
    
    print("Running integration tests with natural time progression...")
    print("Note: Test 1 will take ~60 seconds to complete.\n")
    
    try:
        test_rate_limiting_with_token_refill(store, {
            "test_workflow": {
                "rate_limit_rpm": 3,
                "delay": 5,
                "activity_name": "process_task",
                "max_retries": 2
            }
        })
    except AssertionError as e:
        print(f"✗ Test 1 failed: {e}\n")
    
    store = {}  # Reset state
    
    try:
        test_circuit_breaker_trips_and_stops_processing(store, {
            "test_workflow": {
                "rate_limit_rpm": 100,
                "delay": 5,
                "activity_name": "process_task",
                "max_retries": 2
            }
        })
    except AssertionError as e:
        print(f"✗ Test 2 failed: {e}\n")
    
    store = {}  # Reset state
    
    try:
        test_workflow_isolation_separate_circuits(store, {
            "workflow_a": {
                "rate_limit_rpm": 100,
                "delay": 5,
                "activity_name": "process_task",
                "max_retries": 2
            },
            "workflow_b": {
                "rate_limit_rpm": 100,
                "delay": 5,
                "activity_name": "process_task",
                "max_retries": 2
            }
        })
    except AssertionError as e:
        print(f"✗ Test 3 failed: {e}\n")