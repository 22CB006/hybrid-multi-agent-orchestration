"""Unit tests for agent idempotent event processing."""

import asyncio
from datetime import datetime, timezone
from uuid import UUID

import pytest

from agents.base_agent import BaseDataPlaneAgent
from core.config import Config
from core.schemas import TaskRequest


class MockDataPlaneAgent(BaseDataPlaneAgent):
    """Mock agent implementation for unit testing."""

    def __init__(self, agent_name: str, config: Config):
        super().__init__(agent_name, config)
        self.execution_count = 0

    async def execute_task(self, request: TaskRequest) -> dict:
        """Execute test task and track execution count."""
        self.execution_count += 1
        await asyncio.sleep(0.01)  # Simulate work
        return {
            "status": "success",
            "task_type": request.task_type,
            "execution_count": self.execution_count,
            "payload": request.payload,
        }


@pytest.mark.asyncio
async def test_idempotent_event_processing(test_config):
    """
    Test that duplicate TaskRequest events return cached results without re-execution.

    Validates Requirements 7.1, 7.2, 7.4:
    - 7.1: Agents track processed event IDs to detect duplicates
    - 7.2: Duplicate events skip processing and return cached result
    - 7.4: correlation_id combined with task_type used as deduplication key
    """
    # Create test agent
    agent = MockDataPlaneAgent("test_agent", test_config)

    try:
        # Start agent (connects to Redis and initializes idempotency cache)
        await agent.start()

        # Create first TaskRequest with specific correlation_id and task_type
        correlation_id = UUID("abc12300-0000-0000-0000-000000000000")
        task_request = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_request",
            source_agent="test_client",
            target_agent="test_agent",
            task_type="setup_electricity",
            payload={"address": "123 Main St", "service": "electricity"},
            timeout_seconds=30,
        )

        # Process first request
        await agent.handle_task_request(task_request)

        # Assert task executed once
        assert agent.execution_count == 1, "Task should execute on first request"

        # Check that result was cached
        cached_result = await agent.check_idempotency(
            correlation_id, "setup_electricity"
        )
        assert cached_result is not None, "Result should be cached after execution"
        assert cached_result["status"] == "success"
        assert cached_result["task_type"] == "setup_electricity"
        assert cached_result["execution_count"] == 1

        # Create duplicate TaskRequest with same correlation_id and task_type
        duplicate_request = TaskRequest(
            correlation_id=correlation_id,  # Same correlation_id
            timestamp=datetime.now(timezone.utc),
            event_type="task_request",
            source_agent="test_client",
            target_agent="test_agent",
            task_type="setup_electricity",  # Same task_type
            payload={"address": "123 Main St", "service": "electricity"},
            timeout_seconds=30,
        )

        # Process duplicate request
        await agent.handle_task_request(duplicate_request)

        # Assert task was NOT re-executed (execution_count should still be 1)
        assert agent.execution_count == 1, (
            "Task should NOT re-execute for duplicate request"
        )

        # Verify cached result is still available
        cached_result_after_duplicate = await agent.check_idempotency(
            correlation_id, "setup_electricity"
        )
        assert cached_result_after_duplicate is not None, (
            "Cached result should still exist"
        )
        assert cached_result_after_duplicate["execution_count"] == 1, (
            "Cached result should show original execution count"
        )

        # Test that different task_type with same correlation_id is NOT a duplicate
        different_task_request = TaskRequest(
            correlation_id=correlation_id,  # Same correlation_id
            timestamp=datetime.now(timezone.utc),
            event_type="task_request",
            source_agent="test_client",
            target_agent="test_agent",
            task_type="setup_gas",  # Different task_type
            payload={"address": "123 Main St", "service": "gas"},
            timeout_seconds=30,
        )

        # Process request with different task_type
        await agent.handle_task_request(different_task_request)

        # Assert task WAS executed (execution_count should now be 2)
        assert agent.execution_count == 2, (
            "Task should execute for different task_type even with same correlation_id"
        )

    finally:
        # Cleanup
        await agent.shutdown()


@pytest.mark.asyncio
async def test_main_agent_retry_logic_with_eventual_success(test_config):
    """
    Test Main Agent retry logic with exponential backoff and eventual success.

    Validates Requirements 6.2, 6.3, 6.4:
    - 6.2: Main Agent retries task up to 3 times with exponential backoff
    - 6.3: After all retry attempts fail, event moved to Dead Letter Queue
    - 6.4: Main Agent logs failure with correlation_id when moved to DLQ

    Scenario:
    1. Agent fails with retry_count=0, is_retryable=True
    2. Main Agent retries with 1s delay (retry_count=1)
    3. Agent fails again with retry_count=1
    4. Main Agent retries with 2s delay (retry_count=2)
    5. Agent succeeds with retry_count=2
    6. Workflow completes successfully
    """
    from agents.main_agent import MainAgent
    from core.schemas import TaskFailure, TaskResponse
    from unittest.mock import AsyncMock, patch
    import time

    # Create Main Agent
    main_agent = MainAgent(test_config)

    try:
        # Start Main Agent
        await main_agent.start()

        # Track retry attempts and timing
        retry_attempts = []
        retry_times = []

        # Mock the bus.publish method to capture retry requests
        original_publish = main_agent.bus.publish
        publish_call_count = 0

        async def mock_publish(channel: str, event):
            nonlocal publish_call_count
            publish_call_count += 1

            # Track retry requests
            if isinstance(event, TaskRequest) and "retry_count" in event.payload:
                retry_count = event.payload["retry_count"]
                retry_attempts.append(retry_count)
                retry_times.append(time.time())

            # Call original publish
            await original_publish(channel, event)

        main_agent.bus.publish = mock_publish

        # Create initial workflow
        correlation_id = uuid4()
        user_input = "Set up electricity at 123 Main St"

        # Submit user request (this will publish initial TaskRequest)
        await main_agent.handle_user_request(user_input)

        # Wait for initial request to be published
        await asyncio.sleep(0.1)

        # Simulate first failure (retry_count=0)
        failure_1 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=uuid4(),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=0,
            is_retryable=True,
        )

        start_time = time.time()
        await main_agent.handle_task_failure(failure_1)

        # Wait for retry to be published
        await asyncio.sleep(1.2)  # Wait for 1s delay + processing

        # Verify first retry was attempted with 1s delay
        assert len(retry_attempts) >= 1, "First retry should be attempted"
        assert retry_attempts[0] == 1, "First retry should have retry_count=1"

        # Check timing - should be approximately 1 second delay
        first_retry_delay = retry_times[0] - start_time
        assert 0.9 <= first_retry_delay <= 1.3, (
            f"First retry delay should be ~1s, got {first_retry_delay:.2f}s"
        )

        # Simulate second failure (retry_count=1)
        failure_2 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=uuid4(),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=1,
            is_retryable=True,
        )

        second_failure_time = time.time()
        await main_agent.handle_task_failure(failure_2)

        # Wait for second retry to be published
        await asyncio.sleep(2.2)  # Wait for 2s delay + processing

        # Verify second retry was attempted with 2s delay
        assert len(retry_attempts) >= 2, "Second retry should be attempted"
        assert retry_attempts[1] == 2, "Second retry should have retry_count=2"

        # Check timing - should be approximately 2 second delay
        second_retry_delay = retry_times[1] - second_failure_time
        assert 1.9 <= second_retry_delay <= 2.3, (
            f"Second retry delay should be ~2s, got {second_retry_delay:.2f}s"
        )

        # Simulate success on third attempt (retry_count=2)
        success_response = TaskResponse(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_response",
            source_agent="utilities",
            task_id=uuid4(),
            result={"status": "success", "service": "electricity"},
            duration_ms=150,
        )

        await main_agent._handle_task_response(success_response)

        # Wait for workflow to complete
        await asyncio.sleep(0.2)

        # Verify workflow completed successfully
        workflow = main_agent.active_workflows.get(correlation_id)
        assert workflow is not None, "Workflow should exist"
        assert workflow.status == "completed", "Workflow should be completed"
        assert "utilities" in workflow.completed_agents, (
            "Utilities agent should be in completed agents"
        )
        assert "utilities" in workflow.results, "Results should include utilities"
        assert workflow.results["utilities"]["status"] == "success"

        # Verify exponential backoff pattern: 1s, 2s
        assert len(retry_attempts) == 2, "Should have exactly 2 retry attempts"
        assert retry_attempts == [1, 2], "Retry counts should be [1, 2]"

    finally:
        await main_agent.shutdown()


@pytest.mark.asyncio
async def test_main_agent_retry_exhaustion_moves_to_dlq(test_config):
    """
    Test that after 3 failures, event is moved to Dead Letter Queue.

    Validates Requirements 6.2, 6.3, 6.4:
    - 6.2: Main Agent retries task up to 3 times
    - 6.3: After all retry attempts fail, event moved to Dead Letter Queue
    - 6.4: Main Agent logs failure with correlation_id when moved to DLQ

    Scenario:
    1. Agent fails with retry_count=0
    2. Main Agent retries (retry_count=1)
    3. Agent fails with retry_count=1
    4. Main Agent retries (retry_count=2)
    5. Agent fails with retry_count=2
    6. Main Agent retries (retry_count=3)
    7. Agent fails with retry_count=3
    8. Event moved to DLQ (no more retries)
    """
    from agents.main_agent import MainAgent
    from core.schemas import TaskFailure

    # Create Main Agent
    main_agent = MainAgent(test_config)

    try:
        # Start Main Agent
        await main_agent.start()

        # Track retry attempts
        retry_attempts = []

        # Mock the bus.publish method to capture retry requests
        original_publish = main_agent.bus.publish

        async def mock_publish(channel: str, event):
            # Track retry requests
            if isinstance(event, TaskRequest) and "retry_count" in event.payload:
                retry_count = event.payload["retry_count"]
                retry_attempts.append(retry_count)

            # Call original publish
            await original_publish(channel, event)

        main_agent.bus.publish = mock_publish

        # Create initial workflow
        correlation_id = uuid4()
        user_input = "Set up electricity at 123 Main St"

        # Submit user request
        await main_agent.handle_user_request(user_input)
        await asyncio.sleep(0.1)

        # Simulate failure 1 (retry_count=0)
        failure_1 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=uuid4(),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=0,
            is_retryable=True,
        )
        await main_agent.handle_task_failure(failure_1)
        await asyncio.sleep(1.2)  # Wait for 1s retry delay

        # Verify first retry
        assert len(retry_attempts) == 1, "First retry should be attempted"
        assert retry_attempts[0] == 1

        # Simulate failure 2 (retry_count=1)
        failure_2 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=uuid4(),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=1,
            is_retryable=True,
        )
        await main_agent.handle_task_failure(failure_2)
        await asyncio.sleep(2.2)  # Wait for 2s retry delay

        # Verify second retry
        assert len(retry_attempts) == 2, "Second retry should be attempted"
        assert retry_attempts[1] == 2

        # Simulate failure 3 (retry_count=2)
        failure_3 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=uuid4(),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=2,
            is_retryable=True,
        )
        await main_agent.handle_task_failure(failure_3)
        await asyncio.sleep(4.2)  # Wait for 4s retry delay

        # Verify third retry
        assert len(retry_attempts) == 3, "Third retry should be attempted"
        assert retry_attempts[2] == 3

        # Simulate failure 4 (retry_count=3) - should move to DLQ
        failure_4 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=uuid4(),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=3,
            is_retryable=True,
        )

        # Get DLQ size before
        dlq_events_before = await main_agent.dead_letter_queue.get_all()
        dlq_size_before = len(dlq_events_before)

        await main_agent.handle_task_failure(failure_4)
        await asyncio.sleep(0.2)

        # Verify NO fourth retry (max attempts reached)
        assert len(retry_attempts) == 3, (
            "Should not retry after 3 attempts (max_attempts=3)"
        )

        # Verify event was moved to DLQ
        dlq_events_after = await main_agent.dead_letter_queue.get_all()
        dlq_size_after = len(dlq_events_after)

        assert dlq_size_after == dlq_size_before + 1, (
            "Event should be added to Dead Letter Queue after max retries"
        )

        # Verify the DLQ event contains the failure
        latest_dlq_event = dlq_events_after[-1]
        assert latest_dlq_event.original_event.correlation_id == correlation_id
        assert "Exceeded max retries" in latest_dlq_event.failure_reason

    finally:
        await main_agent.shutdown()


@pytest.mark.asyncio
async def test_main_agent_non_retryable_failure_moves_to_dlq_immediately(test_config):
    """
    Test that non-retryable failures are moved to DLQ immediately without retry.

    Validates Requirements 6.2, 6.3:
    - 6.2: Non-retryable failures skip retry logic
    - 6.3: Non-retryable failures moved to Dead Letter Queue immediately
    """
    from agents.main_agent import MainAgent
    from core.schemas import TaskFailure

    # Create Main Agent
    main_agent = MainAgent(test_config)

    try:
        # Start Main Agent
        await main_agent.start()

        # Track retry attempts
        retry_attempts = []

        # Mock the bus.publish method to capture retry requests
        original_publish = main_agent.bus.publish

        async def mock_publish(channel: str, event):
            # Track retry requests
            if isinstance(event, TaskRequest) and "retry_count" in event.payload:
                retry_count = event.payload["retry_count"]
                retry_attempts.append(retry_count)

            # Call original publish
            await original_publish(channel, event)

        main_agent.bus.publish = mock_publish

        # Create initial workflow
        correlation_id = uuid4()
        user_input = "Set up electricity at 123 Main St"

        # Submit user request
        await main_agent.handle_user_request(user_input)
        await asyncio.sleep(0.1)

        # Get DLQ size before
        dlq_events_before = await main_agent.dead_letter_queue.get_all()
        dlq_size_before = len(dlq_events_before)

        # Simulate non-retryable failure
        non_retryable_failure = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=uuid4(),
            error_type="ValidationError",
            error_message="Invalid address format",
            retry_count=0,
            is_retryable=False,  # Non-retryable
        )

        await main_agent.handle_task_failure(non_retryable_failure)
        await asyncio.sleep(0.2)

        # Verify NO retry attempts
        assert len(retry_attempts) == 0, (
            "Non-retryable failures should not trigger retries"
        )

        # Verify event was moved to DLQ immediately
        dlq_events_after = await main_agent.dead_letter_queue.get_all()
        dlq_size_after = len(dlq_events_after)

        assert dlq_size_after == dlq_size_before + 1, (
            "Non-retryable failure should be added to DLQ immediately"
        )

        # Verify the DLQ event contains the failure
        latest_dlq_event = dlq_events_after[-1]
        assert latest_dlq_event.original_event.correlation_id == correlation_id
        assert "Non-retryable error" in latest_dlq_event.failure_reason

    finally:
        await main_agent.shutdown()


@pytest.mark.asyncio
async def test_main_agent_retry_logic_with_eventual_success(test_config):
    """
    Test Main Agent retry logic with exponential backoff and eventual success.

    Validates Requirements 6.2, 6.3, 6.4:
    - 6.2: Main Agent retries task up to 3 times with exponential backoff
    - 6.3: After all retry attempts fail, event moved to Dead Letter Queue
    - 6.4: Main Agent logs failure with correlation_id when moved to DLQ

    Scenario:
    1. Agent fails with retry_count=0, is_retryable=True
    2. Main Agent retries with 1s delay (retry_count=1)
    3. Agent fails again with retry_count=1
    4. Main Agent retries with 2s delay (retry_count=2)
    5. Agent succeeds with retry_count=2
    6. Workflow completes successfully
    """
    from agents.main_agent import MainAgent
    from core.schemas import TaskFailure, TaskResponse
    import time

    # Create Main Agent
    main_agent = MainAgent(test_config)

    try:
        # Start Main Agent
        await main_agent.start()

        # Track retry attempts and timing
        retry_attempts = []
        retry_times = []

        # Mock the bus.publish method to capture retry requests
        original_publish = main_agent.bus.publish

        async def mock_publish(channel: str, event):
            # Track retry requests
            if isinstance(event, TaskRequest) and "retry_count" in event.payload:
                retry_count = event.payload["retry_count"]
                retry_attempts.append(retry_count)
                retry_times.append(time.time())

            # Call original publish
            await original_publish(channel, event)

        main_agent.bus.publish = mock_publish

        # Create initial workflow
        user_input = "Set up electricity at 123 Main St"

        # Submit user request (this will publish initial TaskRequest and return correlation_id)
        correlation_id = await main_agent.handle_user_request(user_input)

        # Wait for initial request to be published
        await asyncio.sleep(0.1)

        # Simulate first failure (retry_count=0)
        failure_1 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000001"),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=0,
            is_retryable=True,
        )

        start_time = time.time()
        await main_agent.handle_task_failure(failure_1)

        # Wait for retry to be published
        await asyncio.sleep(1.2)  # Wait for 1s delay + processing

        # Verify first retry was attempted with 1s delay
        assert len(retry_attempts) >= 1, "First retry should be attempted"
        assert retry_attempts[0] == 1, "First retry should have retry_count=1"

        # Check timing - should be approximately 1 second delay
        first_retry_delay = retry_times[0] - start_time
        assert 0.9 <= first_retry_delay <= 1.3, (
            f"First retry delay should be ~1s, got {first_retry_delay:.2f}s"
        )

        # Simulate second failure (retry_count=1)
        failure_2 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000002"),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=1,
            is_retryable=True,
        )

        second_failure_time = time.time()
        await main_agent.handle_task_failure(failure_2)

        # Wait for second retry to be published
        await asyncio.sleep(2.2)  # Wait for 2s delay + processing

        # Verify second retry was attempted with 2s delay
        assert len(retry_attempts) >= 2, "Second retry should be attempted"
        assert retry_attempts[1] == 2, "Second retry should have retry_count=2"

        # Check timing - should be approximately 2 second delay
        second_retry_delay = retry_times[1] - second_failure_time
        assert 1.9 <= second_retry_delay <= 2.3, (
            f"Second retry delay should be ~2s, got {second_retry_delay:.2f}s"
        )

        # Simulate success on third attempt (retry_count=2)
        success_response = TaskResponse(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_response",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000003"),
            result={"status": "success", "service": "electricity"},
            duration_ms=150,
        )

        await main_agent._handle_task_response(success_response)

        # Wait for workflow to complete
        await asyncio.sleep(0.2)

        # Verify workflow completed successfully
        workflow = main_agent.active_workflows.get(correlation_id)
        assert workflow is not None, "Workflow should exist"
        assert workflow.status == "completed", "Workflow should be completed"
        assert "utilities" in workflow.completed_agents, (
            "Utilities agent should be in completed agents"
        )
        assert "utilities" in workflow.results, "Results should include utilities"
        assert workflow.results["utilities"]["status"] == "success"

        # Verify exponential backoff pattern: 1s, 2s
        assert len(retry_attempts) == 2, "Should have exactly 2 retry attempts"
        assert retry_attempts == [1, 2], "Retry counts should be [1, 2]"

    finally:
        await main_agent.shutdown()


@pytest.mark.asyncio
async def test_main_agent_retry_exhaustion_moves_to_dlq(test_config):
    """
    Test that after 3 failures, event is moved to Dead Letter Queue.

    Validates Requirements 6.2, 6.3, 6.4:
    - 6.2: Main Agent retries task up to 3 times
    - 6.3: After all retry attempts fail, event moved to Dead Letter Queue
    - 6.4: Main Agent logs failure with correlation_id when moved to DLQ

    Scenario:
    1. Agent fails with retry_count=0
    2. Main Agent retries (retry_count=1)
    3. Agent fails with retry_count=1
    4. Main Agent retries (retry_count=2)
    5. Agent fails with retry_count=2
    6. Main Agent retries (retry_count=3)
    7. Agent fails with retry_count=3
    8. Event moved to DLQ (no more retries)
    """
    from agents.main_agent import MainAgent
    from core.schemas import TaskFailure

    # Create Main Agent
    main_agent = MainAgent(test_config)

    try:
        # Start Main Agent
        await main_agent.start()

        # Track retry attempts
        retry_attempts = []

        # Mock the bus.publish method to capture retry requests
        original_publish = main_agent.bus.publish

        async def mock_publish(channel: str, event):
            # Track retry requests
            if isinstance(event, TaskRequest) and "retry_count" in event.payload:
                retry_count = event.payload["retry_count"]
                retry_attempts.append(retry_count)

            # Call original publish
            await original_publish(channel, event)

        main_agent.bus.publish = mock_publish

        # Create initial workflow
        user_input = "Set up electricity at 123 Main St"

        # Submit user request
        correlation_id = await main_agent.handle_user_request(user_input)
        await asyncio.sleep(0.1)

        # Simulate failure 1 (retry_count=0)
        failure_1 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000011"),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=0,
            is_retryable=True,
        )
        await main_agent.handle_task_failure(failure_1)
        await asyncio.sleep(1.2)  # Wait for 1s retry delay

        # Verify first retry
        assert len(retry_attempts) == 1, "First retry should be attempted"
        assert retry_attempts[0] == 1

        # Simulate failure 2 (retry_count=1)
        failure_2 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000012"),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=1,
            is_retryable=True,
        )
        await main_agent.handle_task_failure(failure_2)
        await asyncio.sleep(2.2)  # Wait for 2s retry delay

        # Verify second retry
        assert len(retry_attempts) == 2, "Second retry should be attempted"
        assert retry_attempts[1] == 2

        # Simulate failure 3 (retry_count=2)
        failure_3 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000013"),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=2,
            is_retryable=True,
        )
        await main_agent.handle_task_failure(failure_3)
        await asyncio.sleep(4.2)  # Wait for 4s retry delay

        # Verify third retry
        assert len(retry_attempts) == 3, "Third retry should be attempted"
        assert retry_attempts[2] == 3

        # Simulate failure 4 (retry_count=3) - should move to DLQ
        failure_4 = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000014"),
            error_type="TransientError",
            error_message="Database connection timeout",
            retry_count=3,
            is_retryable=True,
        )

        # Get DLQ size before
        dlq_events_before = await main_agent.dead_letter_queue.get_all()
        dlq_size_before = len(dlq_events_before)

        await main_agent.handle_task_failure(failure_4)
        await asyncio.sleep(0.2)

        # Verify NO fourth retry (max attempts reached)
        assert len(retry_attempts) == 3, (
            "Should not retry after 3 attempts (max_attempts=3)"
        )

        # Verify event was moved to DLQ
        dlq_events_after = await main_agent.dead_letter_queue.get_all()
        dlq_size_after = len(dlq_events_after)

        assert dlq_size_after == dlq_size_before + 1, (
            "Event should be added to Dead Letter Queue after max retries"
        )

        # Verify the DLQ event contains the failure
        latest_dlq_event = dlq_events_after[-1]
        assert latest_dlq_event.original_event.correlation_id == correlation_id
        assert "Exceeded max retries" in latest_dlq_event.failure_reason

    finally:
        await main_agent.shutdown()


@pytest.mark.asyncio
async def test_main_agent_non_retryable_failure_moves_to_dlq_immediately(test_config):
    """
    Test that non-retryable failures are moved to DLQ immediately without retry.

    Validates Requirements 6.2, 6.3:
    - 6.2: Non-retryable failures skip retry logic
    - 6.3: Non-retryable failures moved to Dead Letter Queue immediately
    """
    from agents.main_agent import MainAgent
    from core.schemas import TaskFailure

    # Create Main Agent
    main_agent = MainAgent(test_config)

    try:
        # Start Main Agent
        await main_agent.start()

        # Track retry attempts
        retry_attempts = []

        # Mock the bus.publish method to capture retry requests
        original_publish = main_agent.bus.publish

        async def mock_publish(channel: str, event):
            # Track retry requests
            if isinstance(event, TaskRequest) and "retry_count" in event.payload:
                retry_count = event.payload["retry_count"]
                retry_attempts.append(retry_count)

            # Call original publish
            await original_publish(channel, event)

        main_agent.bus.publish = mock_publish

        # Create initial workflow
        user_input = "Set up electricity at 123 Main St"

        # Submit user request
        correlation_id = await main_agent.handle_user_request(user_input)
        await asyncio.sleep(0.1)

        # Get DLQ size before
        dlq_events_before = await main_agent.dead_letter_queue.get_all()
        dlq_size_before = len(dlq_events_before)

        # Simulate non-retryable failure
        non_retryable_failure = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="utilities",
            task_id=UUID("00000000-0000-0000-0000-000000000021"),
            error_type="ValidationError",
            error_message="Invalid address format",
            retry_count=0,
            is_retryable=False,  # Non-retryable
        )

        await main_agent.handle_task_failure(non_retryable_failure)
        await asyncio.sleep(0.2)

        # Verify NO retry attempts
        assert len(retry_attempts) == 0, (
            "Non-retryable failures should not trigger retries"
        )

        # Verify event was moved to DLQ immediately
        dlq_events_after = await main_agent.dead_letter_queue.get_all()
        dlq_size_after = len(dlq_events_after)

        assert dlq_size_after == dlq_size_before + 1, (
            "Non-retryable failure should be added to DLQ immediately"
        )

        # Verify the DLQ event contains the failure
        latest_dlq_event = dlq_events_after[-1]
        assert latest_dlq_event.original_event.correlation_id == correlation_id
        assert "Non-retryable error" in latest_dlq_event.failure_reason

    finally:
        await main_agent.shutdown()
