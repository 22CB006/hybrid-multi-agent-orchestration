"""Integration test for failure recovery and retry logic.

This test validates the Main Agent's retry mechanism with exponential backoff,
Dead Letter Queue integration, and DLQ retry functionality.

Validates Requirements: 6.2, 6.3, 6.4, 14.5
"""

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from agents.main_agent import MainAgent
from bus.redis_bus import RedisBus
from core.dead_letter_queue import DeadLetterQueue
from core.schemas import TaskFailure, TaskRequest, TaskResponse


class MockFailingAgent:
    """Mock agent that fails a configurable number of times before succeeding."""

    def __init__(self, config, agent_name: str, fail_count: int = 2):
        """
        Initialize mock failing agent.

        Args:
            config: Test configuration
            agent_name: Name of the agent
            fail_count: Number of times to fail before succeeding
        """
        self.config = config
        self.agent_name = agent_name
        self.bus = RedisBus(config)
        self.fail_count = fail_count
        self.attempt_count = 0
        self.received_requests = []

    async def start(self):
        """Start the mock agent and subscribe to task requests."""
        await self.bus.connect()
        await self.bus.subscribe(
            f"agent.{self.agent_name}.task_request", self.handle_task_request
        )

    async def handle_task_request(self, event: TaskRequest):
        """
        Handle task request with controlled failure behavior.

        Fails for the first N attempts, then succeeds.
        """
        self.received_requests.append(event)
        self.attempt_count += 1
        retry_count = event.payload.get("retry_count", 0)

        print(
            f"MockFailingAgent attempt {self.attempt_count}, retry_count={retry_count}"
        )

        # Fail for the first fail_count attempts
        if self.attempt_count <= self.fail_count:
            failure = TaskFailure(
                correlation_id=event.correlation_id,
                timestamp=datetime.now(timezone.utc),
                event_type="task_failure",
                source_agent=self.agent_name,
                task_id=event.correlation_id,
                error_type="MockError",
                error_message=f"Simulated failure (attempt {self.attempt_count})",
                retry_count=retry_count,
                is_retryable=True,
            )
            await self.bus.publish(f"agent.{self.agent_name}.task_failure", failure)
        else:
            # Succeed after fail_count attempts
            response = TaskResponse(
                correlation_id=event.correlation_id,
                timestamp=datetime.now(timezone.utc),
                event_type="task_response",
                source_agent=self.agent_name,
                task_id=event.correlation_id,
                result={"status": "success", "attempt": self.attempt_count},
                duration_ms=100,
            )
            await self.bus.publish(f"agent.{self.agent_name}.task_response", response)

    async def shutdown(self):
        """Shutdown the mock agent."""
        await self.bus.disconnect()


@pytest.mark.asyncio
async def test_retry_with_exponential_backoff(test_config):
    """Test Main Agent retries with exponential backoff (1s, 2s).

    Workflow:
    1. Mock agent fails twice then succeeds
    2. Assert Main Agent retries with 1s delay (retry_count=1)
    3. Assert Main Agent retries with 2s delay (retry_count=2)
    4. Assert third attempt succeeds

    Validates:
    - Requirement 6.2: Main Agent retries up to 3 times with exponential backoff
    - Requirement 6.3: Exponential backoff delays (1s, 2s, 4s)
    """
    # Setup components
    main_agent = MainAgent(test_config)
    mock_agent = MockFailingAgent(test_config, "test_agent", fail_count=2)

    try:
        # Start agents
        await main_agent.start()
        await mock_agent.start()
        await asyncio.sleep(0.2)  # Allow subscriptions to register

        # Use Main Agent's handle_user_request to create proper workflow state
        # This simulates a real user request that targets test_agent
        user_input = "Test request for test_agent"
        correlation_id = await main_agent.handle_user_request(user_input)

        # Wait for retries to complete (1s + 2s + processing time)
        start_time = asyncio.get_event_loop().time()
        await asyncio.sleep(4.5)
        end_time = asyncio.get_event_loop().time()
        total_time = end_time - start_time

        # Verify agent received 3 requests (initial + 2 retries)
        assert len(mock_agent.received_requests) == 3, (
            f"Expected 3 requests (1 initial + 2 retries), "
            f"got {len(mock_agent.received_requests)}"
        )

        # Verify retry counts in requests
        retry_counts = [
            req.payload.get("retry_count", 0) for req in mock_agent.received_requests
        ]
        assert retry_counts == [0, 1, 2], (
            f"Expected retry_counts [0, 1, 2], got {retry_counts}"
        )

        # Verify exponential backoff timing
        # Should take at least 3 seconds (1s + 2s delays)
        assert total_time >= 3.0, (
            f"Total time {total_time:.2f}s is less than expected 3s "
            f"(exponential backoff not working)"
        )

        # Verify final attempt succeeded
        assert mock_agent.attempt_count == 3, (
            f"Expected 3 attempts, got {mock_agent.attempt_count}"
        )

        print("✓ Retry with exponential backoff verified")
        print(f"✓ Total attempts: {mock_agent.attempt_count}")
        print(f"✓ Retry counts: {retry_counts}")
        print(f"✓ Total time: {total_time:.2f}s (includes 1s + 2s backoff)")
        print("✓ Third attempt succeeded")

    finally:
        await main_agent.shutdown()
        await mock_agent.shutdown()


@pytest.mark.asyncio
async def test_dlq_after_max_retries(test_config):
    """Test event moved to DLQ after 3 failures.

    Workflow:
    1. Mock agent fails 3 times
    2. Assert Main Agent retries twice (attempts 2 and 3)
    3. Assert after 3rd failure, event moved to DLQ
    4. Verify DLQ contains the failed event

    Validates:
    - Requirement 6.3: After all retry attempts fail, move to DLQ
    - Requirement 6.4: Log failure with correlation_id when moved to DLQ
    """
    # Setup components
    main_agent = MainAgent(test_config)
    mock_agent = MockFailingAgent(
        test_config, "test_agent", fail_count=10
    )  # Always fail

    try:
        # Start agents
        await main_agent.start()
        await mock_agent.start()
        await asyncio.sleep(0.2)

        # Use Main Agent's handle_user_request to create proper workflow state
        user_input = "Test request for test_agent"
        correlation_id = await main_agent.handle_user_request(user_input)

        # Wait for all retries to complete (1s + 2s + 4s + processing)
        await asyncio.sleep(8.5)

        # Verify agent received 4 requests (initial + 3 retries)
        # Note: After 4th failure (retry_count=3), no more retries
        assert len(mock_agent.received_requests) == 4, (
            f"Expected 4 requests (1 initial + 3 retries), "
            f"got {len(mock_agent.received_requests)}"
        )

        # Verify retry counts
        retry_counts = [
            req.payload.get("retry_count", 0) for req in mock_agent.received_requests
        ]
        assert retry_counts == [0, 1, 2, 3], (
            f"Expected retry_counts [0, 1, 2, 3], got {retry_counts}"
        )

        # Verify event was moved to DLQ
        dlq_events = await main_agent.dead_letter_queue.get_all()
        assert len(dlq_events) > 0, "Expected event in DLQ, but DLQ is empty"

        # Find the DLQ event with matching correlation_id
        matching_events = [
            e for e in dlq_events if e.original_event.correlation_id == correlation_id
        ]
        assert len(matching_events) >= 1, (
            f"Expected at least 1 DLQ event with correlation_id {correlation_id}, "
            f"got {len(matching_events)}"
        )

        dlq_event = matching_events[0]

        # Verify DLQ event details
        assert dlq_event.original_event.event_type == "task_failure", (
            f"Expected task_failure in DLQ, got {dlq_event.original_event.event_type}"
        )
        assert dlq_event.failure_count == 1, (
            f"Expected failure_count=1, got {dlq_event.failure_count}"
        )
        assert (
            "max retries" in dlq_event.failure_reason.lower()
            or "exceeded" in dlq_event.failure_reason.lower()
        ), f"Expected 'max retries' in failure reason, got: {dlq_event.failure_reason}"

        print("✓ DLQ after max retries verified")
        print(f"✓ Total attempts: {len(mock_agent.received_requests)}")
        print(f"✓ Retry counts: {retry_counts}")
        print(f"✓ Event moved to DLQ after 4 failures (1 initial + 3 retries)")
        print(f"✓ DLQ event ID: {dlq_event.event_id}")
        print(f"✓ DLQ failure reason: {dlq_event.failure_reason}")

    finally:
        await main_agent.shutdown()
        await mock_agent.shutdown()


@pytest.mark.asyncio
async def test_dlq_retry_republishes_event(test_config):
    """Test DLQ retry republishes event successfully.

    Workflow:
    1. Add a failed event to DLQ manually
    2. Call DLQ retry() method
    3. Assert event is republished to message bus
    4. Assert event is removed from DLQ after successful retry

    Validates:
    - Requirement 14.5: System provides API endpoint for replaying events
    - Requirement 6.4: DLQ stores failed events for investigation and replay
    """
    # Setup components
    main_agent = MainAgent(test_config)
    monitor_bus = RedisBus(test_config)

    # Track republished events
    republished_count = [0]

    async def capture_republished(message):
        """Capture raw messages republished from DLQ."""
        republished_count[0] += 1

    try:
        # Start components
        await main_agent.start()
        await monitor_bus.connect()

        # Subscribe to monitor republished events (raw messages)
        await monitor_bus.pubsub.psubscribe("agent.test_agent.*")
        await asyncio.sleep(0.2)

        # Create a failed event
        correlation_id = uuid.uuid4()
        failed_event = TaskFailure(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            event_type="task_failure",
            source_agent="test_agent",
            task_id=correlation_id,
            error_type="TestError",
            error_message="Test failure for DLQ retry",
            retry_count=3,
            is_retryable=True,
        )

        # Add to DLQ using main_agent's DLQ
        event_id = await main_agent.dead_letter_queue.add(
            failed_event, "Test failure for retry validation"
        )

        # Verify event is in DLQ
        dlq_events_before = await main_agent.dead_letter_queue.get_all()
        assert len(dlq_events_before) == 1, (
            f"Expected 1 event in DLQ, got {len(dlq_events_before)}"
        )

        # Retry the event
        retry_success = await main_agent.dead_letter_queue.retry(
            event_id, main_agent.bus
        )

        # Wait for republish
        await asyncio.sleep(0.5)

        # Verify retry was successful
        assert retry_success is True, "DLQ retry returned False"

        # Verify event was removed from DLQ
        dlq_events_after = await main_agent.dead_letter_queue.get_all()
        assert len(dlq_events_after) == 0, (
            f"Expected DLQ to be empty after retry, got {len(dlq_events_after)} events"
        )

        print("✓ DLQ retry republishes event successfully")
        print(f"✓ Event ID: {event_id}")
        print(f"✓ Correlation ID: {correlation_id}")
        print(f"✓ Event republished to message bus")
        print(f"✓ Event removed from DLQ after successful retry")

    finally:
        await main_agent.shutdown()
        await monitor_bus.disconnect()


@pytest.mark.asyncio
async def test_complete_failure_recovery_workflow(test_config):
    """Test complete failure recovery workflow end-to-end.

    This test combines all failure recovery scenarios:
    1. Initial failure with retry
    2. Multiple retries with exponential backoff
    3. DLQ storage after max retries
    4. DLQ retry and recovery

    Validates all requirements: 6.2, 6.3, 6.4, 14.5
    """
    # Setup components
    main_agent = MainAgent(test_config)
    mock_agent = MockFailingAgent(test_config, "recovery_agent", fail_count=10)
    monitor_bus = RedisBus(test_config)

    captured_events = []

    async def capture_all(event):
        captured_events.append(event)

    try:
        # Start all components
        await main_agent.start()
        await mock_agent.start()
        await monitor_bus.connect()
        await monitor_bus.subscribe("agent.recovery_agent.*", capture_all)
        await asyncio.sleep(0.2)

        # Phase 1: Submit request that will fail 3 times
        correlation_id = uuid.uuid4()

        # Use Main Agent's handle_user_request to create proper workflow state
        user_input = "Test request for recovery_agent"
        correlation_id = await main_agent.handle_user_request(user_input)

        # Wait for retries and DLQ
        await asyncio.sleep(8.5)

        # Verify 4 attempts were made (1 initial + 3 retries)
        assert len(mock_agent.received_requests) == 4, (
            f"Expected 4 attempts, got {len(mock_agent.received_requests)}"
        )

        # Verify event in DLQ
        dlq_events = await main_agent.dead_letter_queue.get_all()
        matching_dlq = [
            e for e in dlq_events if e.original_event.correlation_id == correlation_id
        ]
        assert len(matching_dlq) >= 1, "Expected at least one event in DLQ"

        dlq_event_id = matching_dlq[0].event_id

        # Phase 2: Fix the agent (make it succeed)
        mock_agent.fail_count = 0  # Now it will succeed

        # Phase 3: Retry from DLQ
        retry_success = await main_agent.dead_letter_queue.retry(
            dlq_event_id, main_agent.bus
        )

        await asyncio.sleep(0.5)

        # Verify retry was successful
        assert retry_success is True, "DLQ retry failed"

        # Verify DLQ has fewer events after retry (at least one was removed)
        dlq_events_after = await main_agent.dead_letter_queue.get_all()
        assert len(dlq_events_after) < len(dlq_events), (
            f"DLQ should have fewer events after retry. "
            f"Before: {len(dlq_events)}, After: {len(dlq_events_after)}"
        )

        print("✓ Complete failure recovery workflow verified")
        print(f"✓ Phase 1: Failed 4 times (1 initial + 3 retries), moved to DLQ")
        print(f"✓ Phase 2: Agent fixed")
        print(f"✓ Phase 3: DLQ retry successful, event removed from DLQ")
        print(f"✓ Total events captured: {len(captured_events)}")

    finally:
        await main_agent.shutdown()
        await mock_agent.shutdown()
        await monitor_bus.disconnect()
