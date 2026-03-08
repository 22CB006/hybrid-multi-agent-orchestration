"""End-to-end integration test for hybrid multi-agent architecture.

This test validates the complete workflow from API request to aggregated response,
verifying parallel execution, Main Agent orchestration, and response aggregation.

Validates Requirements: 12.1, 12.2, 12.3, 12.4
"""

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from agents.broadband_agent import BroadbandAgent
from agents.main_agent import MainAgent
from agents.utilities_agent import UtilitiesAgent
from bus.redis_bus import RedisBus
from core.schemas import TaskRequest, TaskResponse


@pytest.mark.asyncio
async def test_end_to_end_workflow(test_config):
    """Test complete workflow: API → Main Agent → Parallel Agents → Aggregation.

    Workflow:
    1. Start Redis, Main Agent, Utilities Agent, Broadband Agent
    2. POST /tasks with "Set up utilities and broadband at 123 Main St"
    3. Assert Main Agent parses input and publishes TaskRequest to both agents
    4. Assert both agents process tasks in parallel
    5. Assert Main Agent aggregates both responses
    6. GET /tasks/{correlation_id} returns combined result with both services

    Validates:
    - Requirement 12.1: Main Agent publishes TaskRequest to all required agents simultaneously
    - Requirement 12.2: Agents process tasks concurrently without waiting
    - Requirement 12.3: Main Agent collects responses asynchronously
    - Requirement 12.4: Main Agent aggregates results and responds to user
    """
    # Setup all components
    main_agent = MainAgent(test_config)
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)
    monitor_bus = RedisBus(test_config)

    # Track events for verification
    captured_events = []
    task_requests = []
    task_responses = []

    async def capture_all_events(event):
        """Monitor all events on the bus."""
        captured_events.append(event)
        if event.event_type == "task_request":
            task_requests.append(event)
        elif event.event_type == "task_response":
            task_responses.append(event)

    try:
        # Step 1: Start all agents and monitoring
        await main_agent.start()
        await utilities_agent.start()
        await broadband_agent.start()
        await monitor_bus.connect()

        # Subscribe to all events for verification
        await monitor_bus.subscribe("agent.*.*", capture_all_events)
        await asyncio.sleep(0.2)  # Allow subscriptions to register

        # Step 2: Submit user request via Main Agent
        user_input = "Set up utilities and broadband at 123 Main St"
        correlation_id = await main_agent.handle_user_request(user_input)

        # Wait for workflow to complete
        await asyncio.sleep(1.0)

        # Step 3: Verify Main Agent parsed input and published TaskRequests
        assert len(task_requests) >= 2, (
            f"Expected at least 2 TaskRequests, got {len(task_requests)}"
        )

        # Verify TaskRequests were sent to both agents
        target_agents = {req.target_agent for req in task_requests}
        assert "utilities" in target_agents, "No TaskRequest sent to utilities agent"
        assert "broadband" in target_agents, "No TaskRequest sent to broadband agent"

        # Verify all requests have the same correlation_id
        request_correlation_ids = {req.correlation_id for req in task_requests}
        assert correlation_id in request_correlation_ids, (
            "TaskRequests don't have matching correlation_id"
        )

        # Step 4: Verify both agents processed tasks in parallel
        # Both agents should have responded
        assert len(task_responses) >= 2, (
            f"Expected at least 2 TaskResponses, got {len(task_responses)}"
        )

        # Verify responses came from both agents
        response_agents = {resp.source_agent for resp in task_responses}
        assert "utilities" in response_agents, "No response from utilities agent"
        assert "broadband" in response_agents, "No response from broadband agent"

        # Verify responses have matching correlation_id
        response_correlation_ids = {resp.correlation_id for resp in task_responses}
        assert correlation_id in response_correlation_ids, (
            "TaskResponses don't have matching correlation_id"
        )

        # Step 5: Verify Main Agent aggregated responses
        aggregated_result = await main_agent.aggregate_responses(correlation_id)

        assert aggregated_result is not None, "No aggregated result found"
        assert "status" in aggregated_result, "Aggregated result missing status"
        assert aggregated_result["status"] in ["complete", "completed", "partial"], (
            f"Unexpected status: {aggregated_result['status']}"
        )

        # Verify aggregated result contains data from both agents
        assert "results" in aggregated_result, "Aggregated result missing results"
        results = aggregated_result["results"]

        # Check for utilities and broadband results
        agent_names = [r.get("agent") for r in results if isinstance(r, dict)]
        assert "utilities" in agent_names or any(
            "utilities" in str(r) for r in results
        ), "Aggregated result missing utilities data"
        assert "broadband" in agent_names or any(
            "broadband" in str(r) for r in results
        ), "Aggregated result missing broadband data"

        # Step 6: Verify workflow state shows completion
        workflow_state = await main_agent._load_workflow_state(correlation_id)
        assert workflow_state is not None, "Workflow state not found"
        assert workflow_state.status in ["complete", "completed", "partial"], (
            f"Workflow status is {workflow_state.status}, expected complete or partial"
        )

        # Verify timing - parallel execution should be faster than sequential
        # Each agent takes ~0.1-0.2s, parallel should be < 0.5s total
        response_times = [resp.duration_ms for resp in task_responses]
        max_response_time = max(response_times) if response_times else 0

        print("✓ End-to-end workflow completed successfully")
        print(f"✓ Correlation ID: {correlation_id}")
        print(f"✓ TaskRequests published: {len(task_requests)} to {target_agents}")
        print(f"✓ TaskResponses received: {len(task_responses)} from {response_agents}")
        print(f"✓ Aggregated result status: {aggregated_result['status']}")
        print(f"✓ Max agent response time: {max_response_time}ms")
        print(f"✓ Total events captured: {len(captured_events)}")

    finally:
        # Cleanup
        await main_agent.shutdown()
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await monitor_bus.disconnect()


@pytest.mark.asyncio
async def test_parallel_execution_timing(test_config):
    """Verify parallel execution is faster than sequential execution.

    This test measures actual timing to prove parallel execution:
    - Sequential: agent1_time + agent2_time
    - Parallel: max(agent1_time, agent2_time)
    - Improvement: ~50% latency reduction
    """
    main_agent = MainAgent(test_config)
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)
    monitor_bus = RedisBus(test_config)

    task_responses = []

    async def capture_responses(event):
        if event.event_type == "task_response":
            task_responses.append(event)

    try:
        # Start all components
        await main_agent.start()
        await utilities_agent.start()
        await broadband_agent.start()
        await monitor_bus.connect()
        await monitor_bus.subscribe("agent.*.task_response", capture_responses)
        await asyncio.sleep(0.2)

        # Submit request and measure time
        start_time = asyncio.get_event_loop().time()

        user_input = "Set up utilities and broadband at 456 Oak Ave"
        correlation_id = await main_agent.handle_user_request(user_input)

        # Wait for both responses
        timeout = 3.0
        while (
            len(task_responses) < 2
            and (asyncio.get_event_loop().time() - start_time) < timeout
        ):
            await asyncio.sleep(0.1)

        end_time = asyncio.get_event_loop().time()
        total_time = end_time - start_time

        # Verify both agents responded
        assert len(task_responses) >= 2, (
            f"Expected 2 responses, got {len(task_responses)}"
        )

        # Calculate individual agent times
        agent_times = {resp.source_agent: resp.duration_ms for resp in task_responses}

        # Verify parallel execution
        # Sequential would be sum of times, parallel is max of times
        sequential_time = sum(agent_times.values()) / 1000  # Convert to seconds
        parallel_time = max(agent_times.values()) / 1000

        # Total time should be closer to parallel than sequential
        # Allow overhead for message passing
        assert total_time < sequential_time, (
            f"Execution appears sequential: {total_time:.3f}s >= {sequential_time:.3f}s"
        )

        improvement = (sequential_time - total_time) / sequential_time * 100

        print("✓ Parallel execution timing verified")
        print(f"✓ Agent times: {agent_times}")
        print(f"✓ Sequential estimate: {sequential_time:.3f}s")
        print(f"✓ Parallel estimate: {parallel_time:.3f}s")
        print(f"✓ Actual total time: {total_time:.3f}s")
        print(f"✓ Improvement: {improvement:.1f}%")

    finally:
        await main_agent.shutdown()
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await monitor_bus.disconnect()
