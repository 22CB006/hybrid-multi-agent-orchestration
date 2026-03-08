"""Unit tests for PolicyEnforcer channel isolation policy."""

import pytest
import pytest_asyncio
import redis.asyncio as redis
from datetime import datetime, timezone
from uuid import uuid4

from core.policy_enforcer import PolicyEnforcer, PolicyViolationType
from core.schemas import TaskRequest
from core.config import Config
from core.logger import StructuredLogger


@pytest_asyncio.fixture
async def redis_client():
    """Create Redis client for testing."""
    client = await redis.from_url(
        "redis://localhost:6379", encoding="utf-8", decode_responses=True
    )
    await client.ping()
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def policy_enforcer(redis_client):
    """Create PolicyEnforcer instance for testing."""
    config = Config()
    logger = StructuredLogger("test_policy_enforcer", "DEBUG")
    enforcer = PolicyEnforcer(redis_client, config, logger)
    return enforcer


@pytest.mark.asyncio
async def test_main_agent_can_publish_to_agent_input_channels(policy_enforcer):
    """
    Test that Main Agent can publish to agent input channels.

    Main Agent is the orchestrator and must be able to send task_request
    events to agent input channels like:
    - agent.utilities.task_request
    - agent.broadband.task_request
    """
    # Create TaskRequest from Main Agent
    task_request = TaskRequest(
        correlation_id=uuid4(),
        timestamp=datetime.now(timezone.utc),
        event_type="task_request",
        source_agent="main",  # Main Agent
        target_agent="utilities",
        task_type="setup_electricity",
        payload={"address": "123 Main St"},
        timeout_seconds=30,
    )

    # Test publishing to utilities input channel
    channel = "agent.utilities.task_request"
    is_allowed, violation_reason = await policy_enforcer.check_channel_isolation(
        task_request, channel
    )

    assert is_allowed is True, f"Main Agent should be allowed to publish to {channel}"
    assert violation_reason is None

    # Test publishing to broadband input channel
    channel = "agent.broadband.task_request"
    is_allowed, violation_reason = await policy_enforcer.check_channel_isolation(
        task_request, channel
    )

    assert is_allowed is True, f"Main Agent should be allowed to publish to {channel}"
    assert violation_reason is None


@pytest.mark.asyncio
async def test_data_plane_agent_cannot_publish_to_other_agent_input_channels(
    policy_enforcer,
):
    """
    Test that data plane agents CANNOT publish to other agents' input channels.

    Utilities Agent should NOT be able to publish to:
    - agent.broadband.task_request

    This enforces channel isolation in the data plane.
    """
    # Create TaskRequest from Utilities Agent attempting to publish to Broadband
    task_request = TaskRequest(
        correlation_id=uuid4(),
        timestamp=datetime.now(timezone.utc),
        event_type="task_request",
        source_agent="utilities",  # Utilities Agent
        target_agent="broadband",
        task_type="check_availability",
        payload={"address": "123 Main St"},
        timeout_seconds=30,
    )

    # Attempt to publish to broadband input channel
    channel = "agent.broadband.task_request"
    is_allowed, violation_reason = await policy_enforcer.check_channel_isolation(
        task_request, channel
    )

    assert is_allowed is False, (
        "Utilities Agent should NOT be allowed to publish to broadband input channel"
    )
    assert violation_reason is not None
    assert "Channel isolation violation" in violation_reason
    assert "utilities" in violation_reason
    assert "broadband" in violation_reason


@pytest.mark.asyncio
async def test_agent_can_publish_to_own_output_channels(policy_enforcer):
    """
    Test that agents CAN publish to their own output channels.

    Utilities Agent should be able to publish to:
    - agent.utilities.task_response
    - agent.utilities.task_failure
    - agent.utilities.health_check
    """
    # Create TaskResponse from Utilities Agent
    from core.schemas import TaskResponse

    task_response = TaskResponse(
        correlation_id=uuid4(),
        timestamp=datetime.now(timezone.utc),
        event_type="task_response",
        source_agent="utilities",
        task_id=uuid4(),
        result={"status": "success"},
        duration_ms=1500,
    )

    # Test publishing to own response channel
    channel = "agent.utilities.task_response"
    is_allowed, violation_reason = await policy_enforcer.check_channel_isolation(
        task_response, channel
    )

    assert is_allowed is True, (
        "Agent should be allowed to publish to own output channel"
    )
    assert violation_reason is None
