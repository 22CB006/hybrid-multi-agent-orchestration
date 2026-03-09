"""Policy enforcement for multi-agent communication.

Validates events against defined policies:
- Selective Channel Isolation: Prevents agents from publishing to other agents' 
  input channels (task_request, command, etc.) while allowing whitelisted 
  peer-to-peer data channels (address_validated, etc.). Main Agent can publish 
  to any channel as the system orchestrator.
- Rate Limiting: Enforces maximum event rate per correlation_id

Violations are logged and published as PolicyViolation events.
"""

import re
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional
from uuid import UUID

import redis.asyncio as redis

from core.config import Config
from core.schemas import BaseEvent, PolicyViolation
from core.logger import StructuredLogger


class PolicyViolationType(str, Enum):
    """Types of policy violations that can be detected."""

    CHANNEL_VIOLATION = "channel_violation"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    UNAUTHORIZED_PUBLISH = "unauthorized_publish"


class PolicyEnforcer:
    """
    Enforces communication policies for multi-agent system.

    Validates events against:
    1. Channel Isolation Policy - Agents cannot publish to other agents' input channels
    2. Rate Limit Policy - Maximum 100 events per correlation_id within 60 seconds

    Violations result in logged warnings, PolicyViolation events, and message dropping/throttling.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        config: Config,
        logger: Optional[StructuredLogger] = None,
    ):
        """
        Initialize Policy Enforcer.

        Args:
            redis_client: Connected Redis client instance
            config: Application configuration
            logger: Optional structured logger instance
        """
        self.redis_client = redis_client
        self.config = config
        self.logger = logger or StructuredLogger("policy_enforcer", config.log_level)

        # Policy configuration
        self.rate_limit_max_events = 100  # Max events per correlation_id
        self.rate_limit_window_seconds = 60  # Time window for rate limiting
        self.violation_retention_days = 7  # How long to keep violation history

        # Redis keys
        self.rate_limit_key_prefix = "policy:rate_limit:"
        self.violation_history_key = "policy:violations"
        
        # Allowed peer-to-peer communication channels
        # Format: {source_agent: [allowed_target_channels]}
        self.allowed_peer_channels = {
            "utilities": [
                "agent.broadband.address_validated",  # Utilities can share validation with Broadband
            ],
            "broadband": [
                "agent.utilities.address_validated",  # Broadband can share validation with Utilities
            ],
        }

    async def enforce_policies(
        self, event: BaseEvent, channel: str
    ) -> tuple[bool, Optional[str]]:
        """
        Runs all policy checks on an event.

        Args:
            event: Event to validate
            channel: Channel the event is being published to

        Returns:
            Tuple of (is_allowed, violation_reason)
            - is_allowed: True if event passes all policies, False if violated
            - violation_reason: Description of violation if is_allowed is False

        Side effects:
            - Publishes PolicyViolation event if violation detected
            - Logs violation details
            - Updates violation history in Redis
        """
        # Check Channel Isolation Policy
        channel_allowed, channel_reason = await self.check_channel_isolation(
            event, channel
        )
        if not channel_allowed:
            await self.publish_violation(
                event,
                PolicyViolationType.CHANNEL_VIOLATION,
                channel_reason,
                "message_dropped",
            )
            return False, channel_reason

        # Check Rate Limit Policy
        rate_allowed, rate_reason = await self.check_rate_limit(event)
        if not rate_allowed:
            await self.publish_violation(
                event,
                PolicyViolationType.RATE_LIMIT_EXCEEDED,
                rate_reason,
                "message_throttled",
            )
            return False, rate_reason

        return True, None

    async def check_channel_isolation(
        self, event: BaseEvent, channel: str
    ) -> tuple[bool, Optional[str]]:
        """
        Validates channel naming to prevent direct input channel access.

        Policy:
        - Valid pattern: "agent.{source_agent}.{event_type}"
        - Invalid pattern: "agent.{other_agent}.request" (direct input channel access)
        - Agents MUST NOT publish to other agents' input channels
        - EXCEPTION: Main Agent (source_agent == "main") is exempt and can publish to any channel

        Args:
            event: Event being published
            channel: Target channel name

        Returns:
            Tuple of (is_valid, violation_reason)
        """
        # Parse channel name
        # Expected format: agent.{agent_name}.{event_type}
        channel_pattern = r"^agent\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)$"
        match = re.match(channel_pattern, channel)

        if not match:
            reason = f"Invalid channel format: {channel}. Expected: agent.{{agent_name}}.{{event_type}}"
            self.logger.warning(
                reason, correlation_id=event.correlation_id, channel=channel
            )
            return False, reason

        channel_agent = match.group(1)
        event_type = match.group(2)

        # EXEMPTION: Main Agent is the orchestrator and can publish to any channel
        if event.source_agent == "main":
            return True, None

        # Check if agent is publishing to its own channel
        if channel_agent != event.source_agent:
            # EXEMPTION 2: Check if this is an allowed peer-to-peer channel
            allowed_channels = self.allowed_peer_channels.get(event.source_agent, [])
            if channel in allowed_channels:
                self.logger.debug(
                    f"Allowed peer communication: {event.source_agent} → {channel}",
                    correlation_id=event.correlation_id,
                    source_agent=event.source_agent,
                    target_channel=channel,
                )
                return True, None
            
            # Check if this is a direct input channel access (e.g., "request")
            # Input channels typically use patterns like "task_request", "request", etc.
            input_channel_patterns = ["request", "task_request", "command", "input"]

            if any(pattern in event_type.lower() for pattern in input_channel_patterns):
                reason = (
                    f"Channel isolation violation: Agent '{event.source_agent}' "
                    f"attempted to publish to '{channel_agent}' input channel '{channel}'. "
                    f"Agents must only publish to their own output channels or allowed peer channels."
                )
                self.logger.warning(
                    reason,
                    correlation_id=event.correlation_id,
                    source_agent=event.source_agent,
                    target_channel=channel,
                    violation_type=PolicyViolationType.CHANNEL_VIOLATION.value,
                )
                return False, reason

        # Valid channel access
        return True, None

    async def check_rate_limit(self, event: BaseEvent) -> tuple[bool, Optional[str]]:
        """
        Enforces event rate limits per correlation_id.

        Policy:
        - Maximum 100 events per correlation_id within 60 seconds
        - Tracked in Redis with automatic TTL expiration

        Args:
            event: Event being published

        Returns:
            Tuple of (is_allowed, violation_reason)
        """
        try:
            # Generate rate limit key
            rate_key = f"{self.rate_limit_key_prefix}{event.correlation_id}"

            # Increment counter
            current_count = await self.redis_client.incr(rate_key)

            # Set TTL on first increment
            if current_count == 1:
                await self.redis_client.expire(rate_key, self.rate_limit_window_seconds)

            # Check if limit exceeded
            if current_count > self.rate_limit_max_events:
                reason = (
                    f"Rate limit exceeded: correlation_id '{event.correlation_id}' "
                    f"has {current_count} events in {self.rate_limit_window_seconds}s window "
                    f"(max: {self.rate_limit_max_events})"
                )
                self.logger.warning(
                    reason,
                    correlation_id=event.correlation_id,
                    event_count=current_count,
                    max_events=self.rate_limit_max_events,
                    window_seconds=self.rate_limit_window_seconds,
                    violation_type=PolicyViolationType.RATE_LIMIT_EXCEEDED.value,
                )
                return False, reason

            # Within rate limit
            return True, None

        except Exception as e:
            # Log error but don't block event on rate limit check failure
            self.logger.error(
                f"Rate limit check failed: {e}",
                correlation_id=event.correlation_id,
            )
            # Fail open - allow event if rate limit check fails
            return True, None

    async def publish_violation(
        self,
        event: BaseEvent,
        violation_type: PolicyViolationType,
        violation_details: str,
        action_taken: str,
    ) -> None:
        """
        Emits PolicyViolation event and stores in violation history.

        Args:
            event: Original event that violated policy
            violation_type: Type of policy violated
            violation_details: Description of the violation
            action_taken: Enforcement action applied (e.g., "message_dropped")
        """
        try:
            # Create PolicyViolation event
            violation_event = PolicyViolation(
                correlation_id=event.correlation_id,
                source_agent="policy_enforcer",
                violation_type=violation_type.value,
                violating_agent=event.source_agent,
                violation_details={
                    "original_event_type": event.event_type,
                    "details": violation_details,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                action_taken=action_taken,
            )

            # Store in violation history (Redis list with 7-day retention)
            violation_json = violation_event.model_dump_json()
            await self.redis_client.rpush(self.violation_history_key, violation_json)

            # Set expiration on violation history
            retention_seconds = self.violation_retention_days * 24 * 60 * 60
            await self.redis_client.expire(
                self.violation_history_key, retention_seconds
            )

            self.logger.warning(
                f"Policy violation recorded: {violation_type.value}",
                correlation_id=event.correlation_id,
                violation_type=violation_type.value,
                violating_agent=event.source_agent,
                action_taken=action_taken,
            )

            # Note: In production, this would publish the violation event to the message bus
            # For now, we just log and store it
            # await bus.publish("agent.policy_enforcer.policy_violation", violation_event)

        except Exception as e:
            self.logger.error(
                f"Failed to publish policy violation: {e}",
                correlation_id=event.correlation_id,
            )

    async def get_violation_history(self, limit: int = 100) -> list[PolicyViolation]:
        """
        Retrieves recent policy violations from history.

        Args:
            limit: Maximum number of violations to retrieve (default: 100)

        Returns:
            List of PolicyViolation events, most recent first
        """
        try:
            # Get violations from Redis list (most recent last)
            violations_json = await self.redis_client.lrange(
                self.violation_history_key, -limit, -1
            )

            # Deserialize and reverse to get most recent first
            violations = []
            for violation_json in reversed(violations_json):
                try:
                    violation = PolicyViolation.model_validate_json(violation_json)
                    violations.append(violation)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to deserialize violation: {e}",
                        violation_json=violation_json,
                    )
                    continue

            return violations

        except Exception as e:
            self.logger.error(f"Failed to retrieve violation history: {e}")
            return []

    async def cleanup_expired_violations(self) -> int:
        """
        Removes violations older than retention period.

        Returns:
            Number of violations removed
        """
        try:
            # Calculate expiration threshold
            retention_delta = timedelta(days=self.violation_retention_days)
            expiration_threshold = datetime.now(timezone.utc) - retention_delta

            # Get all violations
            violations_json = await self.redis_client.lrange(
                self.violation_history_key, 0, -1
            )

            # Filter expired violations
            expired_count = 0
            for violation_json in violations_json:
                try:
                    violation = PolicyViolation.model_validate_json(violation_json)
                    if violation.timestamp < expiration_threshold:
                        # Remove from list
                        await self.redis_client.lrem(
                            self.violation_history_key, 1, violation_json
                        )
                        expired_count += 1
                except Exception as e:
                    self.logger.warning(
                        f"Failed to process violation during cleanup: {e}"
                    )
                    continue

            if expired_count > 0:
                self.logger.info(
                    f"Cleaned up {expired_count} expired policy violations",
                    removed_count=expired_count,
                    retention_days=self.violation_retention_days,
                )

            return expired_count

        except Exception as e:
            self.logger.error(f"Failed to cleanup expired violations: {e}")
            return 0
