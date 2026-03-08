"""Agent Registry for tracking health and status of all agents.

Maintains a registry of active agents with health monitoring, heartbeat tracking,
and automatic cleanup of unresponsive agents. Persists state to Redis for durability.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Literal, Optional

import redis.asyncio as redis
from pydantic import BaseModel, Field

from core.logger import StructuredLogger
from core.schemas import HealthCheck


class AgentInfo(BaseModel):
    """Information about a registered agent."""

    agent_name: str = Field(..., description="Unique agent identifier")
    status: Literal["healthy", "degraded", "unhealthy", "unknown"] = Field(
        ..., description="Current agent health status"
    )
    last_heartbeat: datetime = Field(..., description="Timestamp of last heartbeat")
    active_tasks: int = Field(
        default=0, description="Number of tasks currently processing"
    )
    uptime_seconds: int = Field(..., description="Time since agent started")
    consecutive_missed_heartbeats: int = Field(
        default=0, description="Count of consecutive missed heartbeats"
    )
    task_types: List[str] = Field(
        default_factory=list, description="Task types this agent handles"
    )
    keywords: List[str] = Field(
        default_factory=list, description="Keywords that trigger routing to this agent"
    )
    description: str = Field(
        default="", description="Human-readable description of agent capabilities"
    )


class AgentRegistry:
    """
    Tracks health and status of all agents in the system.

    Processes HealthCheck events to maintain agent status, detects unhealthy agents
    based on missed heartbeats, and automatically removes agents that have been
    unresponsive for extended periods.
    """

    # Constants for health monitoring
    HEARTBEAT_INTERVAL = 30  # Expected heartbeat interval in seconds
    MISSED_HEARTBEAT_THRESHOLD = (
        3  # Number of missed heartbeats before marking unhealthy
    )
    UNHEALTHY_TIMEOUT = 90  # Seconds (3 * HEARTBEAT_INTERVAL)
    REMOVAL_TIMEOUT = 300  # Seconds (5 minutes) before removing agent

    def __init__(self, redis_client: redis.Redis, logger: StructuredLogger):
        """
        Initialize Agent Registry.

        Args:
            redis_client: Redis connection for persistence
            logger: Structured logger instance
        """
        self.redis_client = redis_client
        self.logger = logger
        self.agents: Dict[str, AgentInfo] = {}
        self._registry_key = "agent_registry"
        self._health_check_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """
        Start the agent registry and health monitoring.

        Loads existing registry from Redis and starts background health check task.
        """
        # Load existing registry from Redis
        await self._load_from_redis()
        self.logger.info("Agent Registry started")

        # Start background health check task
        self._health_check_task = asyncio.create_task(self._health_check_loop())

    async def stop(self) -> None:
        """Stop the agent registry and cancel background tasks."""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        self.logger.info("Agent Registry stopped")

    async def update_heartbeat(
        self, agent_name: str, health_check: HealthCheck
    ) -> None:
        """
        Update agent status from health check event.

        Args:
            agent_name: Name of agent sending heartbeat
            health_check: HealthCheck event with agent status
        """
        now = datetime.now(timezone.utc)

        # Retrieve or create AgentInfo
        if agent_name in self.agents:
            agent_info = self.agents[agent_name]
            agent_info.last_heartbeat = now
            agent_info.consecutive_missed_heartbeats = 0  # Reset counter
            agent_info.status = health_check.agent_status
            agent_info.active_tasks = health_check.active_tasks
            agent_info.uptime_seconds = health_check.uptime_seconds
        else:
            # New agent registration
            agent_info = AgentInfo(
                agent_name=agent_name,
                status=health_check.agent_status,
                last_heartbeat=now,
                active_tasks=health_check.active_tasks,
                uptime_seconds=health_check.uptime_seconds,
                consecutive_missed_heartbeats=0,
            )
            self.agents[agent_name] = agent_info
            self.logger.info(
                f"New agent registered: {agent_name}",
                correlation_id=health_check.correlation_id,
            )

        # Persist to Redis
        await self._save_to_redis()

        self.logger.debug(
            f"Heartbeat updated for {agent_name}",
            correlation_id=health_check.correlation_id,
            status=agent_info.status,
            active_tasks=agent_info.active_tasks,
        )

    async def check_agent_health(self) -> None:
        """
        Check health of all registered agents.

        Runs periodically to:
        1. Calculate time since last heartbeat
        2. Increment consecutive_missed_heartbeats if threshold exceeded
        3. Mark agents unhealthy after 3 missed heartbeats (90 seconds)
        4. Remove agents with no heartbeat for 5 minutes
        """
        now = datetime.now(timezone.utc)
        agents_to_remove = []

        for agent_name, agent_info in self.agents.items():
            time_since_heartbeat = (now - agent_info.last_heartbeat).total_seconds()

            # Check if agent should be removed (no heartbeat for 5 minutes)
            if time_since_heartbeat >= self.REMOVAL_TIMEOUT:
                agents_to_remove.append(agent_name)
                self.logger.warning(
                    f"Removing agent {agent_name} - no heartbeat for {time_since_heartbeat:.0f}s"
                )
                continue

            # Check if heartbeat is overdue
            if time_since_heartbeat >= self.HEARTBEAT_INTERVAL:
                # Increment missed heartbeat counter
                agent_info.consecutive_missed_heartbeats += 1

                # Mark unhealthy after 3 missed heartbeats
                if (
                    agent_info.consecutive_missed_heartbeats
                    >= self.MISSED_HEARTBEAT_THRESHOLD
                ):
                    if agent_info.status != "unhealthy":
                        agent_info.status = "unhealthy"
                        self.logger.warning(
                            f"Agent {agent_name} marked unhealthy - {agent_info.consecutive_missed_heartbeats} missed heartbeats"
                        )

        # Remove agents that have been unresponsive for too long
        for agent_name in agents_to_remove:
            del self.agents[agent_name]
            self.logger.info(f"Agent {agent_name} removed from registry")

        # Persist changes to Redis
        if agents_to_remove or any(
            agent.consecutive_missed_heartbeats > 0 for agent in self.agents.values()
        ):
            await self._save_to_redis()

    async def get_all_agents(self) -> List[AgentInfo]:
        """
        Retrieve list of all registered agents.

        Returns:
            List of AgentInfo objects for all registered agents
        """
        return list(self.agents.values())

    async def register_capabilities(
        self,
        agent_name: str,
        task_types: List[str],
        keywords: List[str],
        description: str = "",
    ) -> None:
        """
        Register an agent's capabilities (task types, routing keywords, description).

        Args:
            agent_name: Unique agent identifier
            task_types: Task types this agent can handle
            keywords: Keywords that should trigger routing to this agent
            description: Human-readable description of agent capabilities
        """
        # Load existing registry first to avoid overwriting other agents
        await self._load_from_redis()

        if agent_name in self.agents:
            self.agents[agent_name].task_types = task_types
            self.agents[agent_name].keywords = keywords
            self.agents[agent_name].description = description
        else:
            self.agents[agent_name] = AgentInfo(
                agent_name=agent_name,
                status="healthy",
                last_heartbeat=datetime.now(timezone.utc),
                uptime_seconds=0,
                task_types=task_types,
                keywords=keywords,
                description=description,
            )
        await self._save_to_redis()
        self.logger.info(
            f"Registered capabilities for {agent_name}",
            task_types=task_types,
            keywords_count=len(keywords),
            description=description,
        )

    async def get_capabilities_map(self) -> Dict[str, AgentInfo]:
        """
        Return all agents that have registered capabilities, loaded fresh from Redis.

        Returns:
            Dict mapping agent name to AgentInfo for agents with task_types defined
        """
        await self._load_from_redis()
        return {name: info for name, info in self.agents.items() if info.task_types}

    async def get_agent(self, agent_name: str) -> Optional[AgentInfo]:
        """
        Retrieve info for specific agent.

        Args:
            agent_name: Name of agent to retrieve

        Returns:
            AgentInfo if agent is registered, None otherwise
        """
        return self.agents.get(agent_name)

    async def _health_check_loop(self) -> None:
        """
        Background task that runs health checks every 10 seconds.

        Continuously monitors agent health and updates status based on
        missed heartbeats.
        """
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                await self.check_agent_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(
                    f"Error in health check loop: {e}",
                    error_type=type(e).__name__,
                )

    async def _save_to_redis(self) -> None:
        """
        Persist agent registry to Redis.

        Serializes all agent info to JSON and stores in Redis for durability.
        """
        try:
            # Convert agents dict to JSON-serializable format
            registry_data = {
                agent_name: agent_info.model_dump(mode="json")
                for agent_name, agent_info in self.agents.items()
            }

            # Store in Redis
            await self.redis_client.set(
                self._registry_key, json.dumps(registry_data, default=str)
            )

        except Exception as e:
            self.logger.error(
                f"Failed to save registry to Redis: {e}",
                error_type=type(e).__name__,
            )

    async def _load_from_redis(self) -> None:
        """
        Load agent registry from Redis.

        Restores agent state from Redis on startup for durability across restarts.
        """
        try:
            data = await self.redis_client.get(self._registry_key)
            if data:
                registry_data = json.loads(data)
                self.agents = {
                    agent_name: AgentInfo(**agent_data)
                    for agent_name, agent_data in registry_data.items()
                }
                self.logger.info(
                    f"Loaded {len(self.agents)} agents from Redis",
                    agent_count=len(self.agents),
                )
            else:
                self.logger.info("No existing registry found in Redis")

        except Exception as e:
            self.logger.error(
                f"Failed to load registry from Redis: {e}",
                error_type=type(e).__name__,
            )
            # Start with empty registry on load failure
            self.agents = {}
