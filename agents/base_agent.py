"""Base class for data plane agents with idempotent event processing.

Provides common functionality for all specialized agents:
- Redis message bus integration
- Idempotent task processing with Redis cache
- Health check heartbeat publishing
- Graceful shutdown handling
"""

import asyncio
import signal
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional, Set
from uuid import UUID

import redis.asyncio as redis

from bus.redis_bus import RedisBus
from core.agent_registry import AgentRegistry
from core.config import Config
from core.logger import StructuredLogger
from core.schemas import TaskRequest, TaskResponse, TaskFailure, HealthCheck


class BaseDataPlaneAgent(ABC):
    """
    Abstract base class for data plane agents.

    Provides infrastructure for:
    - Direct agent-to-agent communication via Redis Pub/Sub
    - Idempotent event processing with correlation_id tracking
    - Periodic health check publishing
    - Graceful shutdown handling
    - Capability registration for dynamic routing
    """

    # Override in subclasses to declare capabilities
    TASK_TYPES: list[str] = []
    KEYWORDS: list[str] = []
    DESCRIPTION: str = ""

    def __init__(self, agent_name: str, config: Config):
        """
        Initialize base data plane agent.

        Args:
            agent_name: Unique identifier for this agent
            config: Application configuration
        """
        self.agent_name = agent_name
        self.config = config
        self.logger = StructuredLogger(agent_name, config.log_level)

        # Message bus
        self.bus = RedisBus(config, self.logger)

        # Idempotency cache (Redis client for cache operations)
        self.idempotency_cache: Optional[redis.Redis] = None

        # Active task tracking
        self.active_tasks: Set[UUID] = set()

        # Agent lifecycle
        self.start_time = datetime.now(timezone.utc)
        self._shutdown_event = asyncio.Event()
        self._health_check_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """
        Start the data plane agent.

        Performs initialization:
        1. Connects to message bus
        2. Subscribes to agent-specific channels
        3. Starts health check heartbeat loop
        4. Registers signal handlers for graceful shutdown
        """
        self.logger.info(f"Starting {self.agent_name} agent")

        try:
            # Connect to message bus
            await self.bus.connect()

            # Initialize idempotency cache (separate Redis client)
            self.idempotency_cache = await redis.from_url(
                self.config.redis_url, encoding="utf-8", decode_responses=True
            )
            await self.idempotency_cache.ping()
            self.logger.info("Idempotency cache connected")

            # Register capabilities in agent registry
            if self.TASK_TYPES or self.KEYWORDS:
                registry = AgentRegistry(self.idempotency_cache, self.logger)
                await registry.register_capabilities(
                    self.agent_name, self.TASK_TYPES, self.KEYWORDS, self.DESCRIPTION
                )

            # Subscribe to agent-specific task request channel
            task_request_pattern = f"agent.{self.agent_name}.task_request"
            await self.bus.subscribe(task_request_pattern, self.handle_task_request)

            # Start health check heartbeat
            self._health_check_task = asyncio.create_task(self._health_check_loop())

            # Register signal handlers for graceful shutdown (Linux/Mac only)
            import sys

            if sys.platform != "win32":
                loop = asyncio.get_event_loop()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(
                        sig, lambda: asyncio.create_task(self.shutdown())
                    )

            self.logger.info(
                f"{self.agent_name} agent started successfully",
                subscribed_pattern=task_request_pattern,
            )

        except Exception as e:
            self.logger.error(f"Failed to start {self.agent_name} agent: {e}")
            raise

    async def handle_task_request(self, event: TaskRequest) -> None:
        """
        Process incoming task request with idempotency check.

        Flow:
        1. Check idempotency cache for duplicate
        2. If duplicate: return cached result
        3. If new: execute task and cache result
        4. Publish TaskResponse or TaskFailure

        Args:
            event: Task request event from message bus
        """
        correlation_id = event.correlation_id
        task_type = event.task_type

        self.logger.info(
            f"Received task request: {task_type}",
            correlation_id=correlation_id,
            task_type=task_type,
            target_agent=event.target_agent,
        )

        # Check idempotency cache
        cached_result = await self.check_idempotency(correlation_id, task_type)
        if cached_result is not None:
            self.logger.info(
                "Duplicate request detected, returning cached result",
                correlation_id=correlation_id,
                task_type=task_type,
            )

            # Publish cached response
            response = TaskResponse(
                correlation_id=correlation_id,
                timestamp=datetime.now(timezone.utc),
                event_type="task_response",
                source_agent=self.agent_name,
                task_id=correlation_id,
                result=cached_result,
                duration_ms=0,  # Cached response, no execution time
            )
            await self.bus.publish(f"agent.{self.agent_name}.task_response", response)
            return

        # Track active task
        self.active_tasks.add(correlation_id)

        try:
            # Execute task with timeout
            start_time = asyncio.get_event_loop().time()

            result = await asyncio.wait_for(
                self.execute_task(event),
                timeout=event.timeout_seconds,
            )

            end_time = asyncio.get_event_loop().time()
            duration_ms = int((end_time - start_time) * 1000)

            # Store result in idempotency cache
            await self.store_idempotency_result(correlation_id, task_type, result)

            # Publish success response
            response = TaskResponse(
                correlation_id=correlation_id,
                timestamp=datetime.now(timezone.utc),
                event_type="task_response",
                source_agent=self.agent_name,
                task_id=correlation_id,
                result=result,
                duration_ms=duration_ms,
            )

            await self.bus.publish(f"agent.{self.agent_name}.task_response", response)

            self.logger.info(
                f"Task completed successfully: {task_type}",
                correlation_id=correlation_id,
                task_type=task_type,
                duration_ms=duration_ms,
            )

        except asyncio.TimeoutError:
            self.logger.error(
                f"Task timed out after {event.timeout_seconds}s: {task_type}",
                correlation_id=correlation_id,
                task_type=task_type,
                timeout_seconds=event.timeout_seconds,
            )

            # Publish timeout failure
            failure = TaskFailure(
                correlation_id=correlation_id,
                timestamp=datetime.now(timezone.utc),
                event_type="task_failure",
                source_agent=self.agent_name,
                task_id=correlation_id,
                error_type="TimeoutError",
                error_message=f"Task execution exceeded {event.timeout_seconds}s timeout",
                retry_count=event.payload.get("retry_count", 0),
                is_retryable=True,
            )

            await self.bus.publish(f"agent.{self.agent_name}.task_failure", failure)

        except Exception as e:
            self.logger.error(
                f"Task execution failed: {task_type}",
                correlation_id=correlation_id,
                task_type=task_type,
                error_type=type(e).__name__,
                error_message=str(e),
            )

            # Determine if error is retryable
            is_retryable = self._is_retryable_error(e)

            # Publish failure
            failure = TaskFailure(
                correlation_id=correlation_id,
                timestamp=datetime.now(timezone.utc),
                event_type="task_failure",
                source_agent=self.agent_name,
                task_id=correlation_id,
                error_type=type(e).__name__,
                error_message=str(e),
                retry_count=event.payload.get("retry_count", 0),
                is_retryable=is_retryable,
            )

            await self.bus.publish(f"agent.{self.agent_name}.task_failure", failure)

        finally:
            # Remove from active tasks
            self.active_tasks.discard(correlation_id)

    @abstractmethod
    async def execute_task(self, request: TaskRequest) -> dict:
        """
        Execute agent-specific task logic.

        Must be implemented by subclasses to provide business logic.
        Should be idempotent - executing the same task multiple times
        should produce the same result.

        Args:
            request: Task request with task_type and payload

        Returns:
            Task execution result as dictionary

        Raises:
            Exception: On task execution failure
        """
        pass

    async def check_idempotency(
        self, correlation_id: UUID, task_type: str
    ) -> Optional[dict]:
        """
        Check if event was already processed.

        Uses Redis cache with key format: "{correlation_id}:{task_type}"

        Args:
            correlation_id: Request correlation ID
            task_type: Type of task being executed

        Returns:
            Cached result if found, None otherwise
        """
        if not self.idempotency_cache:
            return None

        cache_key = f"{correlation_id}:{task_type}"

        try:
            cached_data = await self.idempotency_cache.get(cache_key)
            if cached_data:
                import json

                return json.loads(cached_data)
            return None

        except Exception as e:
            self.logger.warning(
                f"Failed to check idempotency cache: {e}",
                correlation_id=correlation_id,
                cache_key=cache_key,
            )
            return None

    async def store_idempotency_result(
        self, correlation_id: UUID, task_type: str, result: dict
    ) -> None:
        """
        Cache task result for idempotency.

        Stores result in Redis with TTL from config (default 1 hour).

        Args:
            correlation_id: Request correlation ID
            task_type: Type of task executed
            result: Task execution result to cache
        """
        if not self.idempotency_cache:
            return

        cache_key = f"{correlation_id}:{task_type}"

        try:
            import json

            result_json = json.dumps(result)

            await self.idempotency_cache.setex(
                cache_key,
                self.config.idempotency_ttl,
                result_json,
            )

            self.logger.debug(
                "Stored idempotency result",
                correlation_id=correlation_id,
                cache_key=cache_key,
                ttl=self.config.idempotency_ttl,
            )

        except Exception as e:
            self.logger.warning(
                f"Failed to store idempotency result: {e}",
                correlation_id=correlation_id,
                cache_key=cache_key,
            )

    async def publish_health_check(self) -> None:
        """
        Publish health check heartbeat.

        Sends HealthCheck event with current status and metrics.
        Called periodically by background task.
        """
        uptime_seconds = int(
            (datetime.now(timezone.utc) - self.start_time).total_seconds()
        )

        health_check = HealthCheck(
            correlation_id=UUID("00000000-0000-0000-0000-000000000000"),  # System event
            timestamp=datetime.now(timezone.utc),
            event_type="health_check",
            source_agent=self.agent_name,
            agent_status="healthy",
            active_tasks=len(self.active_tasks),
            uptime_seconds=uptime_seconds,
        )

        try:
            await self.bus.publish(
                f"agent.{self.agent_name}.health_check", health_check
            )

            self.logger.debug(
                "Published health check",
                agent_status="healthy",
                active_tasks=len(self.active_tasks),
                uptime_seconds=uptime_seconds,
            )

        except Exception as e:
            self.logger.error(f"Failed to publish health check: {e}")

    async def _health_check_loop(self) -> None:
        """
        Background task publishing health checks periodically.

        Runs every health_check_interval seconds (default 30s).
        """
        self.logger.info(
            f"Starting health check loop (interval: {self.config.health_check_interval}s)"
        )

        try:
            while not self._shutdown_event.is_set():
                await self.publish_health_check()
                await asyncio.sleep(self.config.health_check_interval)

        except asyncio.CancelledError:
            self.logger.info("Health check loop cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Error in health check loop: {e}")
            raise

    async def shutdown(self) -> None:
        """
        Gracefully shutdown the agent.

        Performs cleanup:
        1. Stops accepting new tasks
        2. Waits for active tasks to complete (max 30s)
        3. Publishes final health check with "unhealthy" status
        4. Disconnects from message bus
        5. Closes idempotency cache connection
        """
        self.logger.info(f"Shutting down {self.agent_name} agent")

        # Signal shutdown
        self._shutdown_event.set()

        # Stop health check loop
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Wait for active tasks to complete (max 30s)
        if self.active_tasks:
            self.logger.info(
                f"Waiting for {len(self.active_tasks)} active tasks to complete"
            )
            try:
                await asyncio.wait_for(
                    self._wait_for_active_tasks(),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Shutdown timeout: {len(self.active_tasks)} tasks still active"
                )

        # Publish final health check
        try:
            final_health_check = HealthCheck(
                correlation_id=UUID("00000000-0000-0000-0000-000000000000"),
                timestamp=datetime.now(timezone.utc),
                event_type="health_check",
                source_agent=self.agent_name,
                agent_status="unhealthy",
                active_tasks=len(self.active_tasks),
                uptime_seconds=int(
                    (datetime.now(timezone.utc) - self.start_time).total_seconds()
                ),
            )
            await self.bus.publish(
                f"agent.{self.agent_name}.health_check", final_health_check
            )
        except Exception as e:
            self.logger.warning(f"Failed to publish final health check: {e}")

        # Disconnect from message bus
        await self.bus.disconnect()

        # Close idempotency cache
        if self.idempotency_cache:
            try:
                await self.idempotency_cache.aclose()
            except Exception as e:
                self.logger.warning(f"Error closing idempotency cache: {e}")

        self.logger.info(f"{self.agent_name} agent shutdown complete")

    async def _wait_for_active_tasks(self) -> None:
        """Wait for all active tasks to complete."""
        while self.active_tasks:
            await asyncio.sleep(0.5)

    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Determine if an error is retryable.

        Args:
            error: Exception that occurred during task execution

        Returns:
            True if error is transient and task should be retried
        """
        # Network errors, timeouts, and temporary failures are retryable
        retryable_types = (
            ConnectionError,
            TimeoutError,
            asyncio.TimeoutError,
        )

        # Check if error type is retryable
        if isinstance(error, retryable_types):
            return True

        # Check error message for retryable indicators
        error_message = str(error).lower()
        retryable_keywords = [
            "timeout",
            "connection",
            "temporary",
            "unavailable",
            "retry",
        ]

        return any(keyword in error_message for keyword in retryable_keywords)
