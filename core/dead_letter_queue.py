"""Dead Letter Queue for failed event storage and retry management.

Stores events that failed processing after retry attempts, providing:
- Persistent storage in Redis
- Event retrieval and inspection
- Manual retry capability
- Automatic cleanup of expired events
"""

import json
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from uuid import UUID, uuid4

import redis.asyncio as redis
from pydantic import BaseModel, Field

from core.config import Config
from core.schemas import BaseEvent
from core.logger import StructuredLogger


class DeadLetterEvent(BaseModel):
    """Failed event with metadata for investigation and retry."""

    event_id: UUID = Field(default_factory=uuid4, description="Unique DLQ event ID")
    original_event: BaseEvent = Field(..., description="The event that failed")
    failure_reason: str = Field(..., description="Why the event failed")
    failure_count: int = Field(default=1, description="Number of times event failed")
    first_failure_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When event first failed",
    )
    last_failure_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When event last failed",
    )
    stack_trace: Optional[str] = Field(
        default=None, description="Stack trace from failure"
    )


class DeadLetterQueue:
    """
    Manages failed events for investigation and potential replay.

    Stores failed events in Redis with automatic expiration and provides
    operations for retrieval, retry, and cleanup.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        config: Config,
        logger: Optional[StructuredLogger] = None,
    ):
        """
        Initialize Dead Letter Queue.

        Args:
            redis_client: Connected Redis client instance
            config: Application configuration
            logger: Optional structured logger instance
        """
        self.redis_client = redis_client
        self.config = config
        self.logger = logger or StructuredLogger("dead_letter_queue", config.log_level)

        # Redis keys
        self.queue_key = "dlq:events"
        self.index_key = "dlq:index"  # Hash mapping event_id -> list index

    async def add(
        self,
        event: BaseEvent,
        failure_reason: str,
        stack_trace: Optional[str] = None,
    ) -> UUID:
        """
        Adds failed event to queue.

        Args:
            event: The event that failed processing
            failure_reason: Description of why the event failed
            stack_trace: Optional stack trace from the failure

        Returns:
            UUID of the DLQ event for tracking

        Raises:
            Exception: If Redis operation fails
        """
        try:
            # Create DLQ event wrapper
            dlq_event = DeadLetterEvent(
                original_event=event,
                failure_reason=failure_reason,
                stack_trace=stack_trace,
            )

            # Serialize to JSON
            dlq_json = dlq_event.model_dump_json()

            # Push to Redis list
            await self.redis_client.rpush(self.queue_key, dlq_json)

            # Store index mapping for fast lookup by event_id
            list_index = await self.redis_client.llen(self.queue_key) - 1
            await self.redis_client.hset(
                self.index_key, str(dlq_event.event_id), str(list_index)
            )

            # Set expiration on the queue (7 days)
            retention_seconds = self.config.dlq_retention_days * 24 * 60 * 60
            await self.redis_client.expire(self.queue_key, retention_seconds)
            await self.redis_client.expire(self.index_key, retention_seconds)

            self.logger.info(
                f"Added event to Dead Letter Queue: {dlq_event.event_id}",
                correlation_id=event.correlation_id,
                event_id=str(dlq_event.event_id),
                failure_reason=failure_reason,
                event_type=event.event_type,
            )

            return dlq_event.event_id

        except Exception as e:
            self.logger.error(
                f"Failed to add event to Dead Letter Queue: {e}",
                correlation_id=event.correlation_id,
            )
            raise

    async def get_all(self) -> List[DeadLetterEvent]:
        """
        Retrieves all events in DLQ.

        Returns:
            List of all DLQ events, ordered by insertion time

        Raises:
            Exception: If Redis operation fails
        """
        try:
            # Get all events from Redis list
            events_json = await self.redis_client.lrange(self.queue_key, 0, -1)

            # Deserialize each event
            events = []
            for event_json in events_json:
                try:
                    dlq_event = DeadLetterEvent.model_validate_json(event_json)
                    events.append(dlq_event)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to deserialize DLQ event: {e}", event_json=event_json
                    )
                    continue

            self.logger.debug(f"Retrieved {len(events)} events from DLQ")
            return events

        except Exception as e:
            self.logger.error(f"Failed to retrieve DLQ events: {e}")
            raise

    async def get_by_id(self, event_id: UUID) -> Optional[DeadLetterEvent]:
        """
        Retrieves specific event by ID.

        Args:
            event_id: UUID of the DLQ event to retrieve

        Returns:
            DLQ event if found, None otherwise

        Raises:
            Exception: If Redis operation fails
        """
        try:
            # Look up index from hash
            index_str = await self.redis_client.hget(self.index_key, str(event_id))

            if index_str is None:
                self.logger.debug(f"DLQ event not found: {event_id}")
                return None

            # Get event from list by index
            event_json = await self.redis_client.lindex(self.queue_key, int(index_str))

            if event_json is None:
                self.logger.warning(
                    f"DLQ event index exists but event not found: {event_id}"
                )
                # Clean up stale index
                await self.redis_client.hdel(self.index_key, str(event_id))
                return None

            # Deserialize event
            dlq_event = DeadLetterEvent.model_validate_json(event_json)

            self.logger.debug(f"Retrieved DLQ event: {event_id}")
            return dlq_event

        except Exception as e:
            self.logger.error(f"Failed to retrieve DLQ event {event_id}: {e}")
            raise

    async def retry(self, event_id: UUID, bus) -> bool:
        """
        Retries failed event by republishing to message bus.

        Args:
            event_id: UUID of the DLQ event to retry
            bus: RedisBus instance for republishing

        Returns:
            True if retry successful, False if event not found

        Raises:
            Exception: If republish operation fails
        """
        try:
            # Retrieve event
            dlq_event = await self.get_by_id(event_id)

            if dlq_event is None:
                self.logger.warning(f"Cannot retry - DLQ event not found: {event_id}")
                return False

            # Determine target channel based on event type
            original_event = dlq_event.original_event
            channel = f"agent.{original_event.source_agent}.{original_event.event_type}"

            # Republish to message bus
            await bus.publish(channel, original_event)

            self.logger.info(
                f"Retried DLQ event: {event_id}",
                correlation_id=original_event.correlation_id,
                event_id=str(event_id),
                channel=channel,
            )

            # Remove from DLQ after successful republish
            await self.remove(event_id)

            return True

        except Exception as e:
            self.logger.error(
                f"Failed to retry DLQ event {event_id}: {e}", event_id=str(event_id)
            )
            raise

    async def remove(self, event_id: UUID) -> bool:
        """
        Removes event from DLQ.

        Args:
            event_id: UUID of the DLQ event to remove

        Returns:
            True if removed, False if not found

        Raises:
            Exception: If Redis operation fails
        """
        try:
            # Look up index
            index_str = await self.redis_client.hget(self.index_key, str(event_id))

            if index_str is None:
                self.logger.debug(f"Cannot remove - DLQ event not found: {event_id}")
                return False

            # Remove from list by setting to placeholder, then removing
            # (Redis doesn't support direct index deletion from list)
            index = int(index_str)
            placeholder = "__DELETED__"
            await self.redis_client.lset(self.queue_key, index, placeholder)
            await self.redis_client.lrem(self.queue_key, 1, placeholder)

            # Remove from index
            await self.redis_client.hdel(self.index_key, str(event_id))

            # Rebuild index since list indices have shifted
            await self._rebuild_index()

            self.logger.info(f"Removed DLQ event: {event_id}", event_id=str(event_id))
            return True

        except Exception as e:
            self.logger.error(f"Failed to remove DLQ event {event_id}: {e}")
            raise

    async def _rebuild_index(self) -> None:
        """
        Rebuilds the event_id -> index mapping after list modifications.

        Internal method to maintain index consistency after removals.
        """
        try:
            # Clear existing index
            await self.redis_client.delete(self.index_key)

            # Get all events
            events_json = await self.redis_client.lrange(self.queue_key, 0, -1)

            # Rebuild index
            for idx, event_json in enumerate(events_json):
                try:
                    dlq_event = DeadLetterEvent.model_validate_json(event_json)
                    await self.redis_client.hset(
                        self.index_key, str(dlq_event.event_id), str(idx)
                    )
                except Exception as e:
                    self.logger.warning(
                        f"Failed to rebuild index for event at position {idx}: {e}"
                    )
                    continue

        except Exception as e:
            self.logger.error(f"Failed to rebuild DLQ index: {e}")
            raise

    async def cleanup_expired(self) -> int:
        """
        Background task removing expired events.

        Removes events older than retention period (7 days by default).

        Returns:
            Number of events removed

        Raises:
            Exception: If Redis operation fails
        """
        try:
            # Calculate expiration threshold
            retention_delta = timedelta(days=self.config.dlq_retention_days)
            expiration_threshold = datetime.now(timezone.utc) - retention_delta

            # Get all events
            events = await self.get_all()

            # Filter expired events
            expired_event_ids = []
            for event in events:
                if event.first_failure_timestamp < expiration_threshold:
                    expired_event_ids.append(event.event_id)

            # Remove expired events
            removed_count = 0
            for event_id in expired_event_ids:
                try:
                    if await self.remove(event_id):
                        removed_count += 1
                except Exception as e:
                    self.logger.warning(
                        f"Failed to remove expired event {event_id}: {e}"
                    )
                    continue

            if removed_count > 0:
                self.logger.info(
                    f"Cleaned up {removed_count} expired DLQ events",
                    removed_count=removed_count,
                    retention_days=self.config.dlq_retention_days,
                )

            return removed_count

        except Exception as e:
            self.logger.error(f"Failed to cleanup expired DLQ events: {e}")
            raise
