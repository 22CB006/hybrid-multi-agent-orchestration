"""Main Agent - Control Plane for hybrid multi-agent architecture.

Provides orchestration, monitoring, policy enforcement, and failure handling
without blocking agent-to-agent communication in the data plane.

Responsibilities:
- Parse user input using Gemini LLM
- Orchestrate multi-agent workflows
- Monitor all agent communication via pattern subscription
- Enforce policies (channel isolation, rate limiting)
- Handle task failures with retry logic
- Maintain agent health registry
- Manage Dead Letter Queue for failed events
"""

import asyncio
import signal
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from uuid import UUID, uuid4

import redis.asyncio as redis
from pydantic import BaseModel, Field

from bus.redis_bus import RedisBus
from core.agent_registry import AgentRegistry
from core.config import Config
from core.dead_letter_queue import DeadLetterQueue
from core.logger import StructuredLogger
from core.policy_enforcer import PolicyEnforcer
from core.schemas import (
    BaseEvent,
    TaskRequest,
    TaskResponse,
    TaskFailure,
    HealthCheck,
    PolicyViolation,
)


class ParsedIntent(BaseModel):
    """Structured output from LLM parser."""

    intent: str = Field(..., description="Primary user intent")
    entities: dict = Field(..., description="Extracted entities")
    target_agents: List[str] = Field(..., description="Agents to handle request")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Parser confidence")


class WorkflowState(BaseModel):
    """State tracking for multi-agent workflows."""

    correlation_id: UUID = Field(..., description="Unique workflow identifier")
    user_input: str = Field(..., description="Original user request")
    parsed_intent: Optional[ParsedIntent] = Field(
        default=None, description="Parsed intent from LLM"
    )
    target_agents: Set[str] = Field(
        default_factory=set, description="Agents assigned to workflow"
    )
    completed_agents: Set[str] = Field(
        default_factory=set, description="Agents that completed tasks"
    )
    pending_agents: Set[str] = Field(
        default_factory=set, description="Agents still processing"
    )
    results: Dict[str, dict] = Field(
        default_factory=dict, description="Results from each agent"
    )
    failures: Dict[str, str] = Field(
        default_factory=dict, description="Failures from each agent"
    )
    status: str = Field(default="in_progress", description="Workflow status")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Workflow creation time",
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Last update time",
    )


class MainAgent:
    """
    Main Agent - Control Plane orchestrator.

    Monitors all agent communication, orchestrates workflows, enforces policies,
    and handles failures without blocking data plane execution.
    """

    def __init__(self, config: Config):
        """
        Initialize Main Agent.

        Args:
            config: Application configuration
        """
        self.config = config
        self.logger = StructuredLogger("main_agent", config.log_level)

        # Message bus
        self.bus = RedisBus(config, self.logger)

        # Redis client for workflow state and components
        self.redis_client: Optional[redis.Redis] = None

        # Control plane components
        self.agent_registry: Optional[AgentRegistry] = None
        self.dead_letter_queue: Optional[DeadLetterQueue] = None
        self.policy_enforcer: Optional[PolicyEnforcer] = None

        # Workflow tracking
        self.active_workflows: Dict[UUID, WorkflowState] = {}
        self._workflow_state_key_prefix = "workflow:state:"

        # Background tasks
        self._health_monitor_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    async def start(self) -> None:
        """
        Start the Main Agent control plane.

        Performs initialization:
        1. Connects to message bus
        2. Subscribes to "agent.*.*" pattern for monitoring
        3. Initializes control plane components
        4. Starts health check monitoring loop
        5. Registers signal handlers for graceful shutdown
        """
        self.logger.info("Starting Main Agent control plane")

        try:
            # Connect to message bus
            await self.bus.connect()

            # Initialize Redis client for state management
            self.redis_client = await redis.from_url(
                self.config.redis_url, encoding="utf-8", decode_responses=True
            )
            await self.redis_client.ping()
            self.logger.info("Redis client connected")

            # Initialize control plane components
            self.agent_registry = AgentRegistry(self.redis_client, self.logger)
            await self.agent_registry.start()

            self.dead_letter_queue = DeadLetterQueue(
                self.redis_client, self.config, self.logger
            )

            self.policy_enforcer = PolicyEnforcer(
                self.redis_client, self.config, self.logger
            )

            # Subscribe to all agent events for monitoring
            await self.bus.subscribe("agent.*.*", self.monitor_events)

            # Start health monitoring background task
            self._health_monitor_task = asyncio.create_task(self.monitor_agent_health())

            # Register signal handlers for graceful shutdown (Linux/Mac only)
            import sys

            if sys.platform != "win32":
                loop = asyncio.get_event_loop()
                for sig in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(
                        sig, lambda: asyncio.create_task(self.shutdown())
                    )

            self.logger.info(
                "Main Agent control plane started successfully",
                monitoring_pattern="agent.*.*",
            )

        except Exception as e:
            self.logger.error(f"Failed to start Main Agent: {e}")
            raise

    async def handle_user_request(self, user_input: str) -> UUID:
        """
        Process user request and orchestrate multi-agent workflow.

        Flow:
        1. Generate correlation_id
        2. Parse input using LLM (placeholder for now)
        3. Create TaskRequest events for each target agent
        4. Publish events to appropriate channels
        5. Initialize workflow state for tracking

        Args:
            user_input: Natural language user request

        Returns:
            correlation_id for status tracking
        """
        correlation_id = uuid4()

        self.logger.info(
            "Processing user request",
            correlation_id=correlation_id,
            user_input=user_input,
        )

        try:
            # Parse input to determine target agents
            # TODO: Implement Gemini parser integration
            # For now, use simple keyword matching
            parsed_intent = self._parse_input_simple(user_input)

            # Create workflow state
            workflow = WorkflowState(
                correlation_id=correlation_id,
                user_input=user_input,
                parsed_intent=parsed_intent,
                target_agents=set(parsed_intent.target_agents),
                pending_agents=set(parsed_intent.target_agents),
            )

            # Store workflow state
            self.active_workflows[correlation_id] = workflow
            await self._save_workflow_state(workflow)

            # Publish TaskRequest to each target agent
            for agent_name in parsed_intent.target_agents:
                # Map generic intent to agent-specific task type
                task_type = parsed_intent.intent
                if agent_name == "utilities":
                    task_type = "setup_electricity"  # Default utilities task
                elif agent_name == "broadband":
                    task_type = "setup_internet"  # Default broadband task

                task_request = TaskRequest(
                    correlation_id=correlation_id,
                    timestamp=datetime.now(timezone.utc),
                    event_type="task_request",
                    source_agent="main",
                    target_agent=agent_name,
                    task_type=task_type,
                    payload=parsed_intent.entities,
                    timeout_seconds=self.config.task_timeout,
                )

                # Publish to agent's task request channel
                channel = f"agent.{agent_name}.task_request"
                await self.bus.publish(channel, task_request)

                self.logger.info(
                    f"Published task request to {agent_name}",
                    correlation_id=correlation_id,
                    agent=agent_name,
                    task_type=task_type,
                )

            return correlation_id

        except Exception as e:
            self.logger.error(
                f"Failed to handle user request: {e}",
                correlation_id=correlation_id,
            )
            raise

    def _parse_input_simple(self, user_input: str) -> ParsedIntent:
        """
        Simple keyword-based parser (placeholder for Gemini integration).

        Args:
            user_input: User's natural language input

        Returns:
            ParsedIntent with target agents and entities
        """
        user_input_lower = user_input.lower()

        # Determine target agents based on keywords
        target_agents = []

        utilities_keywords = ["electricity", "gas", "utilities", "power", "energy"]
        broadband_keywords = ["internet", "broadband", "wifi", "connection"]

        if any(keyword in user_input_lower for keyword in utilities_keywords):
            target_agents.append("utilities")

        if any(keyword in user_input_lower for keyword in broadband_keywords):
            target_agents.append("broadband")

        # Default to both if no specific keywords found
        if not target_agents:
            target_agents = ["utilities", "broadband"]

        # Extract address if present (simple pattern matching)
        import re

        # Try to extract address from input
        address_match = re.search(
            r"\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd)",
            user_input,
            re.IGNORECASE,
        )
        address = address_match.group(0) if address_match else "123 Main Street"

        entities = {"user_input": user_input, "address": address}

        return ParsedIntent(
            intent="setup_services",
            entities=entities,
            target_agents=target_agents,
            confidence=0.8,
        )

    async def monitor_events(self, event: BaseEvent) -> None:
        """
        Callback for pattern subscription "agent.*.*".

        Monitors all events passing through the message bus:
        1. Logs all events with correlation_id
        2. Updates workflow state for TaskResponse/TaskFailure
        3. Detects policy violations
        4. Triggers aggregation when all responses received

        Args:
            event: Event received from message bus
        """
        self.logger.debug(
            f"Monitoring event: {event.event_type}",
            correlation_id=event.correlation_id,
            event_type=event.event_type,
            source_agent=event.source_agent,
        )

        try:
            # Handle different event types
            if isinstance(event, TaskResponse):
                await self._handle_task_response(event)

            elif isinstance(event, TaskFailure):
                await self.handle_task_failure(event)

            elif isinstance(event, HealthCheck):
                await self._handle_health_check(event)

            elif isinstance(event, PolicyViolation):
                await self._handle_policy_violation(event)

        except Exception as e:
            self.logger.error(
                f"Error monitoring event: {e}",
                correlation_id=event.correlation_id,
                event_type=event.event_type,
            )

    async def _handle_task_response(self, response: TaskResponse) -> None:
        """
        Process TaskResponse event and update workflow state.

        Args:
            response: TaskResponse event from data plane agent
        """
        correlation_id = response.correlation_id

        # Check if this is part of an active workflow
        workflow = self.active_workflows.get(correlation_id)
        if not workflow:
            # Try loading from Redis
            workflow = await self._load_workflow_state(correlation_id)
            if workflow:
                self.active_workflows[correlation_id] = workflow

        if not workflow:
            self.logger.debug(
                f"Received response for unknown workflow: {correlation_id}",
                correlation_id=correlation_id,
            )
            return

        # Update workflow state
        agent_name = response.source_agent
        workflow.results[agent_name] = response.result
        workflow.completed_agents.add(agent_name)
        workflow.pending_agents.discard(agent_name)
        workflow.updated_at = datetime.now(timezone.utc)

        # Save updated state
        await self._save_workflow_state(workflow)

        self.logger.info(
            f"Task response received from {agent_name}",
            correlation_id=correlation_id,
            agent=agent_name,
            duration_ms=response.duration_ms,
        )

        # Check if workflow is complete
        if not workflow.pending_agents:
            workflow.status = "completed"
            await self._save_workflow_state(workflow)

            self.logger.info(
                "Workflow completed",
                correlation_id=correlation_id,
                completed_agents=list(workflow.completed_agents),
                failed_agents=list(workflow.failures.keys()),
            )

            # Aggregate results
            await self.aggregate_responses(correlation_id)

    async def _handle_health_check(self, health_check: HealthCheck) -> None:
        """
        Process HealthCheck event and update agent registry.

        Args:
            health_check: HealthCheck event from data plane agent
        """
        if self.agent_registry:
            await self.agent_registry.update_heartbeat(
                health_check.source_agent, health_check
            )

    async def _handle_policy_violation(self, violation: PolicyViolation) -> None:
        """
        Process PolicyViolation event.

        Args:
            violation: PolicyViolation event from policy enforcer
        """
        self.logger.warning(
            f"Policy violation detected: {violation.violation_type}",
            correlation_id=violation.correlation_id,
            violating_agent=violation.violating_agent,
            action_taken=violation.action_taken,
        )

    async def aggregate_responses(self, correlation_id: UUID) -> dict:
        """
        Collect all responses for a workflow.

        Combines results from multiple agents and handles partial failures.

        Args:
            correlation_id: Workflow identifier

        Returns:
            Combined response with results from all agents
        """
        workflow = self.active_workflows.get(correlation_id)
        if not workflow:
            workflow = await self._load_workflow_state(correlation_id)

        if not workflow:
            self.logger.warning(
                f"Cannot aggregate - workflow not found: {correlation_id}",
                correlation_id=correlation_id,
            )
            return {}

        # Combine results
        aggregated = {
            "correlation_id": str(correlation_id),
            "status": workflow.status,
            "results": workflow.results,
            "failures": workflow.failures,
            "completed_agents": list(workflow.completed_agents),
            "failed_agents": list(workflow.failures.keys()),
        }

        self.logger.info(
            "Aggregated workflow results",
            correlation_id=correlation_id,
            successful_agents=len(workflow.results),
            failed_agents=len(workflow.failures),
        )

        return aggregated

    async def handle_task_failure(self, failure: TaskFailure) -> None:
        """
        Process task failures with retry logic.

        Flow:
        1. Check retry count
        2. If retry_count < 3: publish retry with exponential backoff
        3. If retry_count >= 3: move to Dead Letter Queue
        4. Update workflow state

        Args:
            failure: TaskFailure event from data plane agent
        """
        correlation_id = failure.correlation_id
        agent_name = failure.source_agent
        retry_count = failure.retry_count

        self.logger.warning(
            f"Task failure from {agent_name}",
            correlation_id=correlation_id,
            agent=agent_name,
            error_type=failure.error_type,
            error_message=failure.error_message,
            retry_count=retry_count,
            is_retryable=failure.is_retryable,
        )

        # Update workflow state
        workflow = self.active_workflows.get(correlation_id)
        if workflow:
            workflow.failures[agent_name] = failure.error_message
            workflow.pending_agents.discard(agent_name)
            workflow.updated_at = datetime.now(timezone.utc)
            await self._save_workflow_state(workflow)

        # Check if retryable
        if not failure.is_retryable:
            self.logger.error(
                "Task is not retryable, moving to DLQ",
                correlation_id=correlation_id,
                agent=agent_name,
            )
            if self.dead_letter_queue:
                await self.dead_letter_queue.add(
                    failure, f"Non-retryable error: {failure.error_message}"
                )
            return

        # Check retry count
        if retry_count >= self.config.retry_max_attempts:
            self.logger.error(
                f"Task failed after {retry_count} retries, moving to DLQ",
                correlation_id=correlation_id,
                agent=agent_name,
            )
            if self.dead_letter_queue:
                await self.dead_letter_queue.add(
                    failure, f"Exceeded max retries ({self.config.retry_max_attempts})"
                )
            return

        # Retry with exponential backoff
        delay = self.config.retry_base_delay * (2**retry_count)

        self.logger.info(
            f"Retrying task after {delay}s delay (attempt {retry_count + 1})",
            correlation_id=correlation_id,
            agent=agent_name,
            delay_seconds=delay,
        )

        await asyncio.sleep(delay)

        # Get original task request from workflow
        if workflow and workflow.parsed_intent:
            retry_request = TaskRequest(
                correlation_id=correlation_id,
                timestamp=datetime.now(timezone.utc),
                event_type="task_request",
                source_agent="main",
                target_agent=agent_name,
                task_type=workflow.parsed_intent.intent,
                payload={
                    **workflow.parsed_intent.entities,
                    "retry_count": retry_count + 1,
                },
                timeout_seconds=self.config.task_timeout,
            )

            # Republish task request
            channel = f"agent.{agent_name}.task_request"
            await self.bus.publish(channel, retry_request)

            # Update workflow state
            workflow.pending_agents.add(agent_name)
            await self._save_workflow_state(workflow)

            self.logger.info(
                f"Retry task published to {agent_name}",
                correlation_id=correlation_id,
                agent=agent_name,
                retry_count=retry_count + 1,
            )

    async def monitor_agent_health(self) -> None:
        """
        Background task monitoring agent health.

        Runs continuously, checking agent registry for unhealthy agents
        and logging health status changes.
        """
        self.logger.info("Starting agent health monitoring")

        try:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(10)  # Check every 10 seconds

                if self.agent_registry:
                    # Health checks are performed by AgentRegistry internally
                    agents = await self.agent_registry.get_all_agents()

                    # Log unhealthy agents
                    unhealthy_agents = [
                        agent for agent in agents if agent.status == "unhealthy"
                    ]

                    if unhealthy_agents:
                        self.logger.warning(
                            f"Unhealthy agents detected: {len(unhealthy_agents)}",
                            unhealthy_agents=[
                                agent.agent_name for agent in unhealthy_agents
                            ],
                        )

        except asyncio.CancelledError:
            self.logger.info("Agent health monitoring cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Error in agent health monitoring: {e}")
            raise

    async def detect_policy_violations(self, event: BaseEvent) -> None:
        """
        Analyze events for policy violations.

        Delegates to PolicyEnforcer for validation.

        Args:
            event: Event to validate against policies
        """
        if not self.policy_enforcer:
            return

        # Determine channel from event
        channel = f"agent.{event.source_agent}.{event.event_type}"

        # Enforce policies
        is_allowed, violation_reason = await self.policy_enforcer.enforce_policies(
            event, channel
        )

        if not is_allowed:
            self.logger.warning(
                f"Policy violation detected: {violation_reason}",
                correlation_id=event.correlation_id,
                source_agent=event.source_agent,
            )

    async def shutdown(self) -> None:
        """
        Gracefully shutdown the Main Agent.

        Performs cleanup:
        1. Stops accepting new requests
        2. Waits for active workflows to complete (max 30s)
        3. Stops background tasks
        4. Unsubscribes from all channels
        5. Disconnects from message bus
        6. Closes Redis connections
        """
        self.logger.info("Shutting down Main Agent control plane")

        # Signal shutdown
        self._shutdown_event.set()

        # Stop health monitoring task
        if self._health_monitor_task and not self._health_monitor_task.done():
            self._health_monitor_task.cancel()
            try:
                await self._health_monitor_task
            except asyncio.CancelledError:
                pass

        # Wait for active workflows to complete (max 30s)
        if self.active_workflows:
            self.logger.info(
                f"Waiting for {len(self.active_workflows)} active workflows to complete"
            )
            try:
                await asyncio.wait_for(
                    self._wait_for_active_workflows(),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Shutdown timeout: {len(self.active_workflows)} workflows still active"
                )

        # Stop agent registry
        if self.agent_registry:
            await self.agent_registry.stop()

        # Disconnect from message bus
        await self.bus.disconnect()

        # Close Redis client
        if self.redis_client:
            try:
                await self.redis_client.aclose()
            except Exception as e:
                self.logger.warning(f"Error closing Redis client: {e}")

        self.logger.info("Main Agent control plane shutdown complete")

    async def _wait_for_active_workflows(self) -> None:
        """Wait for all active workflows to complete."""
        while self.active_workflows:
            # Check if any workflows are still pending
            pending_workflows = [
                wf for wf in self.active_workflows.values() if wf.pending_agents
            ]

            if not pending_workflows:
                break

            await asyncio.sleep(0.5)

    async def _save_workflow_state(self, workflow: WorkflowState) -> None:
        """
        Persist workflow state to Redis.

        Args:
            workflow: Workflow state to save
        """
        if not self.redis_client:
            return

        try:
            key = f"{self._workflow_state_key_prefix}{workflow.correlation_id}"

            # Convert sets to lists for JSON serialization
            workflow_dict = workflow.model_dump(mode="json")
            workflow_dict["target_agents"] = list(workflow.target_agents)
            workflow_dict["completed_agents"] = list(workflow.completed_agents)
            workflow_dict["pending_agents"] = list(workflow.pending_agents)

            # Store with 1 hour TTL
            import json

            await self.redis_client.setex(
                key, 3600, json.dumps(workflow_dict, default=str)
            )

        except Exception as e:
            self.logger.warning(
                f"Failed to save workflow state: {e}",
                correlation_id=workflow.correlation_id,
            )

    async def _load_workflow_state(
        self, correlation_id: UUID
    ) -> Optional[WorkflowState]:
        """
        Load workflow state from Redis.

        Args:
            correlation_id: Workflow identifier

        Returns:
            WorkflowState if found, None otherwise
        """
        if not self.redis_client:
            return None

        try:
            key = f"{self._workflow_state_key_prefix}{correlation_id}"
            data = await self.redis_client.get(key)

            if not data:
                return None

            import json

            workflow_dict = json.loads(data)

            # Convert lists back to sets
            workflow_dict["target_agents"] = set(workflow_dict["target_agents"])
            workflow_dict["completed_agents"] = set(workflow_dict["completed_agents"])
            workflow_dict["pending_agents"] = set(workflow_dict["pending_agents"])

            return WorkflowState(**workflow_dict)

        except Exception as e:
            self.logger.warning(
                f"Failed to load workflow state: {e}",
                correlation_id=correlation_id,
            )
            return None
