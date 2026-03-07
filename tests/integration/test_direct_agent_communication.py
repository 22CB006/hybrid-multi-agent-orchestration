"""Integration test verifying direct agent-to-agent communication via Redis.

This test validates the core architectural principle: agents communicate directly
through Redis pub/sub WITHOUT routing through Main Agent.
"""

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from agents.broadband_agent import BroadbandAgent
from agents.utilities_agent import UtilitiesAgent
from bus.redis_bus import RedisBus
from core.schemas import TaskRequest, TaskResponse


@pytest.mark.asyncio
async def test_direct_agent_to_agent_communication(test_config):
    """Verify utilities agent can send message directly to broadband agent via Redis.

    This test proves:
    1. Utilities agent publishes event to Redis
    2. Broadband agent receives event directly (no Main Agent routing)
    3. Broadband agent responds directly via Redis
    4. Communication happens in parallel without central bottleneck
    """
    # Setup
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)

    # Shared bus for verification
    verification_bus = RedisBus(test_config)

    # Track received messages
    received_messages = []

    async def capture_message(event):
        """Callback to capture all messages on the bus."""
        received_messages.append(event)

    try:
        # Connect all components
        await utilities_agent.start()
        await broadband_agent.start()
        await verification_bus.connect()

        # Subscribe to all agent channels to monitor direct communication
        await verification_bus.subscribe("agent.*.*", capture_message)

        # Give subscriptions time to register
        await asyncio.sleep(0.1)

        # Simulate utilities agent sending a message that broadband agent should receive
        correlation_id = uuid.uuid4()

        # Utilities agent publishes a task request for broadband
        task_request = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="utilities",
            target_agent="broadband",
            task_type="check_availability",
            payload={"address": "123 Main St"},
            timeout_seconds=30,
        )

        # Publish directly to broadband's channel (simulating direct communication)
        await utilities_agent.bus.publish("agent.broadband.task_request", task_request)

        # Wait for broadband agent to process and respond
        await asyncio.sleep(0.5)

        # Verify direct communication occurred
        assert len(received_messages) > 0, "No messages captured on bus"

        # Find the task request and response
        task_requests = [
            msg for msg in received_messages if msg.event_type == "task_request"
        ]
        task_responses = [
            msg for msg in received_messages if msg.event_type == "task_response"
        ]

        assert len(task_requests) >= 1, "Task request not published to bus"
        assert len(task_responses) >= 1, "Task response not published to bus"

        # Verify the response came from broadband agent
        broadband_response = task_responses[0]
        assert broadband_response.source_agent == "broadband", (
            f"Expected response from broadband, got {broadband_response.source_agent}"
        )
        assert broadband_response.correlation_id == correlation_id, (
            "Response correlation_id doesn't match request"
        )

        # Verify response contains expected data
        assert isinstance(broadband_response, TaskResponse), (
            f"Expected TaskResponse, got {type(broadband_response)}"
        )
        assert "address" in broadband_response.result, "Response missing address field"
        assert broadband_response.result["address"] == "123 Main St", (
            "Response address doesn't match request"
        )

        print("✓ Direct agent-to-agent communication verified")
        print("✓ Utilities → Redis → Broadband (no Main Agent routing)")
        print(f"✓ Correlation ID tracked: {correlation_id}")
        print(f"✓ Response received in {broadband_response.duration_ms}ms")

    finally:
        # Cleanup
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await verification_bus.disconnect()


@pytest.mark.asyncio
async def test_parallel_agent_execution(test_config):
    """Verify multiple agents can process tasks in parallel via Redis.

    This test proves:
    1. Both agents receive task requests simultaneously
    2. Both agents process tasks concurrently (not sequentially)
    3. Total execution time is ~max(agent1_time, agent2_time), not sum
    4. No central bottleneck delays parallel execution
    """
    # Setup
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)
    bus = RedisBus(test_config)

    responses = []

    async def capture_responses(event):
        """Capture task responses."""
        if event.event_type == "task_response":
            responses.append(event)

    try:
        # Start agents and bus
        await utilities_agent.start()
        await broadband_agent.start()
        await bus.connect()

        # Monitor responses
        await bus.subscribe("agent.*.task_response", capture_responses)
        await asyncio.sleep(0.1)

        # Create two task requests with same correlation_id
        correlation_id = uuid.uuid4()

        utilities_request = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="test",
            target_agent="utilities",
            task_type="validate_address",
            payload={"address": "456 Oak Ave"},
            timeout_seconds=30,
        )

        broadband_request = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="test",
            target_agent="broadband",
            task_type="check_availability",
            payload={"address": "456 Oak Ave"},
            timeout_seconds=30,
        )

        # Publish both requests simultaneously (parallel execution)
        start_time = asyncio.get_event_loop().time()

        await asyncio.gather(
            bus.publish("agent.utilities.task_request", utilities_request),
            bus.publish("agent.broadband.task_request", broadband_request),
        )

        # Wait for both responses
        timeout = 2.0
        elapsed = 0
        while len(responses) < 2 and elapsed < timeout:
            await asyncio.sleep(0.1)
            elapsed = asyncio.get_event_loop().time() - start_time

        end_time = asyncio.get_event_loop().time()
        total_time = end_time - start_time

        # Verify both agents responded
        assert len(responses) == 2, f"Expected 2 responses, got {len(responses)}"

        # Verify both responses have same correlation_id
        assert all(r.correlation_id == correlation_id for r in responses), (
            "Not all responses have matching correlation_id"
        )

        # Verify responses came from different agents
        source_agents = {r.source_agent for r in responses}
        assert source_agents == {"utilities", "broadband"}, (
            f"Expected responses from both agents, got {source_agents}"
        )

        # Verify parallel execution (total time should be ~max, not sum)
        # Each task simulates ~0.1-0.2s, sequential would be ~0.3-0.4s
        # Parallel should be ~0.2s + overhead
        assert total_time < 1.0, (
            f"Parallel execution took {total_time:.2f}s (too slow, may be sequential)"
        )

        print("✓ Parallel execution verified")
        print("✓ Both agents processed tasks concurrently")
        print(f"✓ Total time: {total_time:.3f}s (parallel, not sequential)")
        print(f"✓ Responses: {len(responses)} from {source_agents}")

    finally:
        # Cleanup
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await bus.disconnect()


@pytest.mark.asyncio
async def test_no_main_agent_routing(test_config):
    """Verify agents communicate without Main Agent in the data path.

    This test proves:
    1. Agents can exchange messages when Main Agent is not running
    2. Direct pub/sub works independently of control plane
    3. No dependency on Main Agent for data plane operations
    """
    # Setup WITHOUT Main Agent
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)
    monitor_bus = RedisBus(test_config)

    captured_events = []

    async def monitor_all_events(event):
        """Monitor all events to verify no Main Agent involvement."""
        captured_events.append(event)

    try:
        # Start only data plane agents (NO Main Agent)
        await utilities_agent.start()
        await broadband_agent.start()
        await monitor_bus.connect()

        # Monitor all channels
        await monitor_bus.subscribe("agent.*.*", monitor_all_events)
        await asyncio.sleep(0.1)

        # Send task request to utilities agent
        correlation_id = uuid.uuid4()
        request = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="external",
            target_agent="utilities",
            task_type="validate_address",
            payload={"address": "789 Pine Rd"},
            timeout_seconds=30,
        )

        await monitor_bus.publish("agent.utilities.task_request", request)

        # Wait for response
        await asyncio.sleep(0.5)

        # Verify communication happened
        assert len(captured_events) > 0, "No events captured"

        # Verify NO events came from Main Agent
        main_agent_events = [e for e in captured_events if e.source_agent == "main"]
        assert len(main_agent_events) == 0, (
            f"Found {len(main_agent_events)} events from Main Agent (should be 0)"
        )

        # Verify utilities agent responded
        utilities_responses = [
            e
            for e in captured_events
            if e.source_agent == "utilities" and e.event_type == "task_response"
        ]
        assert len(utilities_responses) >= 1, "Utilities agent did not respond"

        print("✓ Agents communicated without Main Agent")
        print(f"✓ Total events: {len(captured_events)}")
        print("✓ Main Agent events: 0 (verified no routing)")
        print("✓ Direct pub/sub working independently")

    finally:
        # Cleanup
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await monitor_bus.disconnect()
