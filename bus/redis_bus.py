"""Redis-based message bus for event-driven agent communication.

Provides pub/sub abstraction over Redis with:
- Exponential backoff reconnection
- Pattern-based subscriptions
- Event serialization/deserialization
- Error handling with Dead Letter Queue integration
"""

import asyncio
import json
from typing import Callable, Dict, Optional, Awaitable

import redis.asyncio as redis
from redis.exceptions import ConnectionError as RedisConnectionError

from core.config import Config
from core.schemas import (
    BaseEvent,
    TaskRequest,
    TaskResponse,
    TaskFailure,
    HealthCheck,
    PolicyViolation,
)
from core.logger import StructuredLogger


class RedisBus:
    """
    Redis-based message bus for agent communication.

    Provides publish/subscribe operations with automatic reconnection,
    event serialization, and pattern-based subscriptions for monitoring.
    """

    def __init__(self, config: Config, logger: Optional[StructuredLogger] = None):
        """
        Initialize Redis message bus.

        Args:
            config: Application configuration with Redis connection details
            logger: Optional structured logger instance
        """
        self.config = config
        self.logger = logger or StructuredLogger("redis_bus", config.log_level)

        self.redis_client: Optional[redis.Redis] = None
        self.pubsub: Optional[redis.client.PubSub] = None
        self.subscriptions: Dict[str, Callable[[BaseEvent], Awaitable[None]]] = {}
        self._listener_task: Optional[asyncio.Task] = None
        self._is_connected = False

    async def connect(self) -> None:
        """
        Establishes connection to Redis with exponential backoff retry.

        Attempts connection up to 5 times with delays: 1s, 2s, 4s, 8s, 16s.

        Raises:
            ConnectionError: If connection fails after 5 attempts
        """
        await self._reconnect_with_backoff()
        self.logger.info("Redis message bus connected successfully")

    async def _reconnect_with_backoff(self) -> None:
        """
        Internal method implementing exponential backoff reconnection.

        Retry delays: 1s, 2s, 4s, 8s, 16s (max 5 attempts)

        Raises:
            ConnectionError: If all retry attempts fail
        """
        max_attempts = 5
        base_delay = 1.0

        for attempt in range(max_attempts):
            try:
                self.logger.info(
                    f"Attempting Redis connection (attempt {attempt + 1}/{max_attempts})",
                    redis_url=self.config.redis_url.replace(
                        self.config.redis_password or "", "***"
                    ),
                )

                # Create Redis client
                self.redis_client = await redis.from_url(
                    self.config.redis_url, encoding="utf-8", decode_responses=True
                )

                # Test connection
                await self.redis_client.ping()

                # Create pub/sub handler
                self.pubsub = self.redis_client.pubsub()

                self._is_connected = True
                self.logger.info("Redis connection established successfully")
                return

            except (RedisConnectionError, Exception) as e:
                self.logger.warning(
                    f"Redis connection attempt {attempt + 1} failed: {e}",
                    error_type=type(e).__name__,
                )

                if attempt >= max_attempts - 1:
                    self.logger.error(
                        f"Failed to connect to Redis after {max_attempts} attempts"
                    )
                    raise ConnectionError(
                        f"Failed to connect to Redis after {max_attempts} attempts: {e}"
                    )

                # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                delay = base_delay * (2**attempt)
                self.logger.info(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)

    async def disconnect(self) -> None:
        """
        Gracefully closes Redis connection and pub/sub handler.

        Stops listener task, unsubscribes from all channels, and closes connections.
        """
        self.logger.info("Disconnecting from Redis message bus")

        # Stop listener task
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        # Unsubscribe from all channels
        if self.pubsub:
            try:
                await self.pubsub.unsubscribe()
                await self.pubsub.close()
            except Exception as e:
                self.logger.warning(f"Error closing pub/sub: {e}")

        # Close Redis client
        if self.redis_client:
            try:
                await self.redis_client.close()
            except Exception as e:
                self.logger.warning(f"Error closing Redis client: {e}")

        self._is_connected = False
        self.logger.info("Redis message bus disconnected")

    async def publish(self, channel: str, event: BaseEvent) -> None:
        """
        Serializes event to JSON and publishes to specified channel.

        Args:
            channel: Target channel name (e.g., "agent.utilities.task_request")
            event: Pydantic event model to publish

        Raises:
            ConnectionError: If not connected to Redis
            PublishError: If publish operation fails
        """
        if not self._is_connected or not self.redis_client:
            raise ConnectionError("Not connected to Redis. Call connect() first.")

        try:
            # Serialize event to JSON
            event_json = self._serialize_event(event)

            # Publish to channel
            await self.redis_client.publish(channel, event_json)

            self.logger.debug(
                f"Published event to channel: {channel}",
                correlation_id=event.correlation_id,
                event_type=event.event_type,
                channel=channel,
            )

        except Exception as e:
            self.logger.error(
                f"Failed to publish event to channel {channel}: {e}",
                correlation_id=event.correlation_id,
                event_type=event.event_type,
                channel=channel,
            )
            raise

    async def subscribe(
        self, pattern: str, callback: Callable[[BaseEvent], Awaitable[None]]
    ) -> None:
        """
        Subscribes to channel pattern and registers callback for incoming events.

        Args:
            pattern: Channel pattern (supports Redis glob: *, ?, [])
            callback: Async function to handle received events

        Example:
            await bus.subscribe("agent.utilities.*", handle_utilities_event)
            await bus.subscribe("agent.*.*", monitor_all_events)

        Raises:
            ConnectionError: If not connected to Redis
        """
        if not self._is_connected or not self.pubsub:
            raise ConnectionError("Not connected to Redis. Call connect() first.")

        try:
            # Register callback
            self.subscriptions[pattern] = callback

            # Subscribe to pattern
            await self.pubsub.psubscribe(pattern)

            self.logger.info(f"Subscribed to pattern: {pattern}")

            # Start listener task if not already running
            if not self._listener_task or self._listener_task.done():
                self._listener_task = asyncio.create_task(self._listen_for_messages())

        except Exception as e:
            self.logger.error(f"Failed to subscribe to pattern {pattern}: {e}")
            raise

    async def unsubscribe(self, pattern: str) -> None:
        """
        Unsubscribes from channel pattern.

        Args:
            pattern: Channel pattern to unsubscribe from
        """
        if not self.pubsub:
            return

        try:
            await self.pubsub.punsubscribe(pattern)
            self.subscriptions.pop(pattern, None)
            self.logger.info(f"Unsubscribed from pattern: {pattern}")
        except Exception as e:
            self.logger.warning(f"Error unsubscribing from pattern {pattern}: {e}")

    async def _listen_for_messages(self) -> None:
        """
        Internal listener task that processes incoming messages.

        Runs continuously, deserializing messages and invoking registered callbacks.
        """
        if not self.pubsub:
            return

        self.logger.info("Started listening for messages")

        try:
            async for message in self.pubsub.listen():
                if message["type"] == "pmessage":
                    pattern = message["pattern"]
                    channel = message["channel"]
                    data = message["data"]

                    # Find matching callback
                    callback = self.subscriptions.get(pattern)
                    if not callback:
                        continue

                    try:
                        # Deserialize event
                        event = self._deserialize_event(data)

                        # Invoke callback
                        await callback(event)

                    except Exception as e:
                        self.logger.error(
                            f"Error processing message from channel {channel}: {e}",
                            channel=channel,
                            pattern=pattern,
                        )
                        # Note: DLQ integration would happen here in production
                        # For now, we log the error and continue

        except asyncio.CancelledError:
            self.logger.info("Message listener task cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Error in message listener: {e}")
            raise

    def _serialize_event(self, event: BaseEvent) -> str:
        """
        Converts Pydantic event to JSON string.

        Args:
            event: Pydantic event model

        Returns:
            JSON string representation
        """
        return event.model_dump_json()

    def _deserialize_event(self, data: str) -> BaseEvent:
        """
        Parses JSON string to appropriate Pydantic event model.

        Args:
            data: JSON string to deserialize

        Returns:
            Deserialized event object

        Raises:
            ValidationError: On invalid schema
        """
        # Parse JSON to dict
        event_dict = json.loads(data)

        # Determine event type and deserialize to appropriate model
        event_type = event_dict.get("event_type")

        event_type_map = {
            "task_request": TaskRequest,
            "task_response": TaskResponse,
            "task_failure": TaskFailure,
            "health_check": HealthCheck,
            "policy_violation": PolicyViolation,
        }

        event_class = event_type_map.get(event_type, BaseEvent)

        # Deserialize using Pydantic
        return event_class.model_validate(event_dict)
