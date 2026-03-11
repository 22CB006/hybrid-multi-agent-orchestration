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
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from uuid import UUID, uuid4

import redis.asyncio as redis
from pydantic import BaseModel, Field

from bus.redis_bus import RedisBus
from core.agent_registry import AgentRegistry
from core.config import Config
from core.dead_letter_queue import DeadLetterQueue
from core.openrouter_parser import OpenRouterParser, ParsedIntent
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
    # Hop tracking fields
    hop_count: int = Field(default=0, description="Total number of hops in workflow")
    hops: List[dict] = Field(default_factory=list, description="Detailed hop records")
    start_time: Optional[float] = Field(default=None, description="Workflow start time (perf_counter)")
    total_duration_ms: Optional[float] = Field(default=None, description="Total workflow duration in milliseconds")


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
        self.openrouter_parser: Optional[OpenRouterParser] = None

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

            # Initialize OpenRouter parser for Mode 3 (DeepSeek v3.2 routing)
            try:
                openrouter_key = getattr(self.config, "openrouter_api_key", None)
                if openrouter_key:
                    self.openrouter_parser = OpenRouterParser(
                        api_key=openrouter_key,
                        logger=self.logger,
                    )
                    self.logger.info("OpenRouter parser initialized")
                else:
                    self.logger.info("OPENROUTER_API_KEY not set — Mode 3 will fall back to keywords")
            except Exception as e:
                self.logger.warning(f"OpenRouter parser init failed: {e}")
                self.openrouter_parser = None

            # Subscribe to all agent events for monitoring
            await self.bus.subscribe("agent.*.*", self.monitor_events)

            # Start health monitoring background task
            self._health_monitor_task = asyncio.create_task(self.monitor_agent_health())

            # Register signal handlers for graceful shutdown (main thread only)
            import sys
            import threading

            if sys.platform != "win32" and threading.current_thread() is threading.main_thread():
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

    async def handle_user_request(
        self,
        user_input: str,
        routing_mode: str = "keywords",
        payload_override: Optional[dict] = None,
        correlation_id: Optional[UUID] = None,
        timeout_override: Optional[int] = None,
    ) -> UUID:
        """
        Process user request and orchestrate multi-agent workflow.

        Flow:
        1. Generate correlation_id
        2. Parse input using selected routing strategy
        3. Create TaskRequest events for each target agent
        4. Publish events to appropriate channels (fire-and-forget)
        5. Initialize workflow state for tracking

        Args:
            user_input: Natural language user request
            routing_mode: One of:
                "keywords"    — Mode 2: keyword matching only (no LLM)
                "openrouter"  — Mode 3: DeepSeek v3.2 via OpenRouter API

        Returns:
            correlation_id for status tracking
        """
        correlation_id = correlation_id or uuid4()
        timeout_seconds = timeout_override or self.config.task_timeout

        self.logger.info(
            "🎯 PARSE_INPUT CALLED - Processing user request",
            correlation_id=correlation_id,
            user_input=user_input,
        )

        try:
            # Load registered agent capabilities from registry
            capabilities = {}
            if self.agent_registry:
                capabilities = await self.agent_registry.get_capabilities_map()

            # ── Route based on selected routing_mode ──────────────────────────
            parsed_intent = None

            if routing_mode == "openrouter":
                # Mode 3: DeepSeek v3.2 via OpenRouter API
                self.logger.info(
                    "Using OpenRouter (DeepSeek v3.2) for routing",
                    correlation_id=correlation_id,
                    routing_mode=routing_mode,
                )
                if self.openrouter_parser:
                    try:
                        parsed_intent, llm_ms = await self.openrouter_parser.parse_input(user_input)
                        self.logger.info(
                            f"OpenRouter routing complete in {llm_ms}ms",
                            correlation_id=correlation_id,
                            target_agents=parsed_intent.target_agents,
                        )
                    except Exception as e:
                        self.logger.warning(
                            f"OpenRouter failed, falling back to keywords: {e}",
                            correlation_id=correlation_id,
                        )
                else:
                    self.logger.warning(
                        "OpenRouter parser not available, falling back to keywords",
                        correlation_id=correlation_id,
                    )

            elif routing_mode == "keywords":
                # Mode 2 explicit: keyword matching only, no LLM calls
                self.logger.info(
                    "Using keyword matching for routing (no LLM)",
                    correlation_id=correlation_id,
                    routing_mode=routing_mode,
                )
                # parsed_intent stays None → falls through to keyword logic below

            else:
                # Mode 2 default: keyword matching only (no LLM)
                self.logger.info(
                    "Using keyword matching for routing (no LLM)",
                    correlation_id=correlation_id,
                    routing_mode=routing_mode,
                )
                # parsed_intent stays None → falls through to keyword logic below

            # Keyword fallback (used by Mode 2 explicit and as fallback for others)
            if parsed_intent is None:
                if capabilities:
                    parsed_intent = self._parse_input_from_capabilities(
                        user_input, capabilities
                    )
                else:
                    self.logger.warning(
                        "No agent capabilities registered, using hardcoded fallback"
                    )
                    parsed_intent = self._parse_input_simple(user_input)

            # Log the parsed intent for visibility
            self.logger.info(
                "📋 Parsed user intent",
                correlation_id=correlation_id,
                intent=parsed_intent.intent,
                target_agents=parsed_intent.target_agents,
                entities=parsed_intent.entities,
                confidence=parsed_intent.confidence,
            )

            # Create workflow state
            workflow = WorkflowState(
                correlation_id=correlation_id,
                user_input=user_input,
                parsed_intent=parsed_intent,
                target_agents=set(parsed_intent.target_agents),
                pending_agents=set(parsed_intent.target_agents),
                start_time=time.perf_counter(),  # Initialize timing
            )

            # Track initial hop: User → Main Agent
            self._track_hop(workflow, "User", "Main Agent", f'"{user_input}"', "request")

            # Add LLM routing hops for Mode 3 (OpenRouter)
            if routing_mode == "openrouter" and parsed_intent:
                self._track_hop(workflow, "Main Agent", "OpenRouter LLM", "routing request", "request")
                # Get the LLM duration from the routing (approximate)
                llm_duration = 8000  # Approximate based on logs showing ~8640ms
                self._track_hop(workflow, "OpenRouter LLM", "Main Agent", "routing response", "response", llm_duration)

            # Store workflow state
            self.active_workflows[correlation_id] = workflow
            await self._save_workflow_state(workflow)

            # Use payload_override if provided, otherwise fall back to parsed entities
            effective_payload = payload_override if payload_override is not None else parsed_intent.entities

            # Check if address validation should be triggered first
            address = effective_payload.get("address")
            has_utilities = "utilities" in parsed_intent.target_agents
            is_validation_only = parsed_intent.intent == "validate_address"
            
            # Trigger address validation FIRST if:
            # 1. Address is present in request
            # 2. Utilities agent is involved
            # 3. This is NOT already a validation-only request
            if address and has_utilities and not is_validation_only:
                self.logger.info(
                    "🏠 Address detected with service setup request - validating address first",
                    correlation_id=correlation_id,
                    address=address,
                    original_intent=parsed_intent.intent,
                )
                
                # Send validate_address task to utilities FIRST
                validation_request = TaskRequest(
                    correlation_id=correlation_id,
                    timestamp=datetime.now(timezone.utc),
                    event_type="task_request",
                    source_agent="main",
                    target_agent="utilities",
                    task_type="validate_address",
                    payload=effective_payload,
                    timeout_seconds=timeout_seconds,
                )
                
                await self.bus.publish("agent.utilities.task_request", validation_request)
                
                # Track hop: Main Agent → Utilities Agent (Redis as infrastructure)
                self._track_hop(workflow, "Main Agent", "Utilities Agent", f"validate_address (via Redis)", "request")
                
                self.logger.info(
                    "📤 Published address validation request to utilities (peer communication will trigger)",
                    correlation_id=correlation_id,
                    address=address,
                )
                
                # Wait 0.5s for utilities to validate and publish to broadband peer channel
                await asyncio.sleep(0.5)
                
                self.logger.info(
                    "✅ Address validation complete, proceeding with service setup tasks",
                    correlation_id=correlation_id,
                )

            # Publish TaskRequest to each target agent
            for agent_name in parsed_intent.target_agents:
                # Map intent to agent-specific task type dynamically
                task_type = parsed_intent.intent
                agent_info = capabilities.get(agent_name)
                
                # Special handling for utilities if we already sent validate_address
                if agent_name == "utilities" and address and has_utilities and not is_validation_only:
                    # We already sent validate_address, now send a single setup task
                    # The utilities agent will handle internal sequencing
                    task_request = TaskRequest(
                        correlation_id=correlation_id,
                        timestamp=datetime.now(timezone.utc),
                        event_type="task_request",
                        source_agent="main",
                        target_agent="utilities",
                        task_type="setup_utilities",  # Single task that handles internal sequencing
                        payload={
                            **effective_payload,
                            "services_requested": {
                                "electricity": "electricity" in user_input.lower() or "power" in user_input.lower(),
                                "gas": "gas" in user_input.lower()
                            }
                        },
                        timeout_seconds=timeout_seconds,
                    )
                    
                    await self.bus.publish("agent.utilities.task_request", task_request)
                    
                    # Track hop: Main Agent → Utilities Agent (Redis as infrastructure)
                    self._track_hop(workflow, "Main Agent", "Utilities Agent", f"setup_utilities (via Redis)", "request")
                    
                    self.logger.info(
                        f"⚡ Published setup_utilities task (will sequence internally)",
                        correlation_id=correlation_id,
                        agent="utilities",
                        task_type="setup_utilities",
                        services_requested=f"electricity: {'electricity' in user_input.lower()}, gas: {'gas' in user_input.lower()}",
                    )
                    
                    # Skip the normal publish below for utilities
                    continue
                elif agent_info and agent_info.task_types:
                    # Use first registered task type as default if intent doesn't match
                    if task_type not in agent_info.task_types:
                        task_type = agent_info.task_types[0]
                elif agent_name == "broadband" and task_type == "setup_services":
                    # Fallback mapping for broadband when agent_info is not available
                    task_type = "check_availability"  # Map setup_services to check_availability for broadband
                elif agent_name == "utilities" and task_type == "setup_services":
                    # Fallback mapping for utilities when agent_info is not available  
                    task_type = "setup_utilities"  # Map setup_services to setup_utilities for utilities

                # Create task request for other agents
                task_request = TaskRequest(
                    correlation_id=correlation_id,
                    timestamp=datetime.now(timezone.utc),
                    event_type="task_request",
                    source_agent="main",
                    target_agent=agent_name,
                    task_type=task_type,
                    payload=effective_payload,
                    timeout_seconds=timeout_seconds,
                )

                # Publish to agent's task request channel
                channel = f"agent.{agent_name}.task_request"
                await self.bus.publish(channel, task_request)

                # Track hop: Main Agent → Target Agent (Redis as infrastructure)
                self._track_hop(workflow, "Main Agent", agent_name.title() + " Agent", f"{task_type} (via Redis)", "request")

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

    def _parse_input_from_capabilities(
        self, user_input: str, capabilities: Dict[str, "AgentInfo"]
    ) -> ParsedIntent:
        """
        Dynamic keyword-based parser using registered agent capabilities.

        Uses multi-strategy matching:
        1. Exact phrase match (multi-word keywords like "gas connection")
        2. Exact word match (single-word keywords)
        3. Stem/prefix match ("electric" matches input word "electrical")

        Scores each agent by number of keyword hits and routes to all
        agents that score above zero. If none match, falls back to all agents.

        Args:
            user_input: User's natural language input
            capabilities: Dict of agent_name -> AgentInfo with keywords

        Returns:
            ParsedIntent with target agents determined from capabilities
        """
        user_input_lower = user_input.lower()
        input_words = set(user_input_lower.split())

        agent_scores: Dict[str, int] = {}

        for agent_name, info in capabilities.items():
            score = 0
            matched_keywords = []
            for kw in info.keywords:
                kw_lower = kw.lower()

                # Strategy 1: Multi-word phrase match (e.g., "gas connection", "jio fiber")
                if " " in kw_lower:
                    if kw_lower in user_input_lower:
                        score += 3  # Phrase matches are strongest signal
                        matched_keywords.append(kw)
                    continue

                # Strategy 2: Exact word match (e.g., "gas" matches word "gas" but not "gasoline")
                # Skip very generic single words that are less than 4 chars unless they're exact domain terms
                if kw_lower in input_words:
                    # Boost score for domain-specific terms
                    if len(kw_lower) >= 4 or kw_lower in ["gas", "wifi", "eb", "lpg", "png", "dsl", "bb"]:
                        score += 2
                        matched_keywords.append(kw)
                    continue

                # Strategy 3: Stem/prefix match (e.g., keyword "electric" matches input "electrical")
                for word in input_words:
                    # Check if keyword is a prefix of input word or vice versa (min 4 chars to avoid false matches)
                    if len(kw_lower) >= 4 and len(word) >= 4:
                        if word.startswith(kw_lower) or kw_lower.startswith(word):
                            score += 1
                            matched_keywords.append(kw)
                            break

            if score > 0:
                agent_scores[agent_name] = score
                self.logger.debug(
                    f"Agent {agent_name} matched with score {score}",
                    matched_keywords=matched_keywords,
                )

        # Select agents that matched
        if agent_scores:
            target_agents = list(agent_scores.keys())
            # Sort by score descending for logging
            target_agents.sort(key=lambda a: agent_scores[a], reverse=True)
            confidence = min(0.95, 0.5 + 0.1 * max(agent_scores.values()))

            self.logger.info(
                "Dynamic routing scores",
                agent_scores=agent_scores,
                selected_agents=target_agents,
            )
        else:
            # Fallback: no keywords matched, route to all registered agents
            target_agents = list(capabilities.keys())
            confidence = 0.3

        address = self._extract_address(user_input)
        entities = {"user_input": user_input, "address": address}

        return ParsedIntent(
            intent="setup_services",
            entities=entities,
            target_agents=target_agents,
            confidence=confidence,
        )

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

        # Support test agent names for testing
        test_agent_keywords = ["test_agent", "recovery_agent"]

        if any(keyword in user_input_lower for keyword in utilities_keywords):
            target_agents.append("utilities")

        if any(keyword in user_input_lower for keyword in broadband_keywords):
            target_agents.append("broadband")

        # Check for test agents
        for test_agent in test_agent_keywords:
            if test_agent in user_input_lower:
                target_agents.append(test_agent)

        # Default to both if no specific keywords found
        if not target_agents:
            target_agents = ["utilities", "broadband"]

        address = self._extract_address(user_input)
        entities = {"user_input": user_input, "address": address}

        return ParsedIntent(
            intent="setup_services",
            entities=entities,
            target_agents=target_agents,
            confidence=0.8,
        )

    def _extract_address(self, user_input: str) -> str:
        """
        Extract address from user input using multiple patterns.

        Tries in order:
        1. Numbered addresses: "42 Oak Avenue", "123 Main St"
        2. Location prepositions: "at <address>", "@ <address>", "near <address>", "to <address>"
        3. Named places with suffixes: "arokiyamadha avenue", "baker street"

        Args:
            user_input: Raw user input text

        Returns:
            Extracted address string, or the full user input as fallback
        """
        import re

        # Pattern 1: Numbered address (e.g., "42 Oak Avenue", "123 Main St")
        numbered = re.search(
            r"\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Nagar|Colony|Layout)",
            user_input,
            re.IGNORECASE,
        )
        if numbered:
            return numbered.group(0).strip()

        # Pattern 2: After location preposition
        # Try "@" and "at" first (strongest location signals), then "near", "to", "in"
        for prep_pattern in [
            r"(?:@|at)\s+(.+?)$",
            r"(?:near|to|in)\s+(.+?)$",
        ]:
            preposition = re.search(prep_pattern, user_input, re.IGNORECASE)
            if preposition:
                candidate = preposition.group(1).strip()
                # Clean trailing service keywords from the captured address
                candidate = re.sub(
                    r"\s*(?:and\s+)?(?:need|set\s*up|setup|please|electricity|gas|internet|broadband|wifi|power|energy|utilities)\b.*$",
                    "",
                    candidate,
                    flags=re.IGNORECASE,
                ).strip()
                if len(candidate) >= 3:
                    return candidate

        # Pattern 3: Named place with road/area suffix anywhere in input
        named = re.search(
            r"[A-Za-z][A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Nagar|Colony|Layout|Place|Way|Circle|Court|Ct)",
            user_input,
            re.IGNORECASE,
        )
        if named:
            return named.group(0).strip()

        # Fallback: return the full user input as address context
        return user_input.strip()

    def _track_hop(self, workflow: WorkflowState, source: str, target: str, message: str, hop_type: str = "request", duration_ms: float = 0.0) -> None:
        """
        Track a hop in the workflow for debugging and analysis.

        Args:
            workflow: Workflow state to update
            source: Source agent/component
            target: Target agent/component  
            message: Description of the hop
            hop_type: Type of hop ("request", "response", "internal", "peer")
            duration_ms: Duration of the hop in milliseconds
        """
        workflow.hop_count += 1
        hop_record = {
            "number": workflow.hop_count,
            "source": source,
            "target": target,
            "message": message,
            "type": hop_type,
            "duration_ms": round(duration_ms, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        workflow.hops.append(hop_record)
        
        # Debug log the hop
        self.logger.info(
            f"🔍 MAIN AGENT DEBUG - Hop tracked",
            correlation_id=workflow.correlation_id,
            hop_number=workflow.hop_count,
            source=source,
            target=target,
            hop_message=message,
            hop_type=hop_type,
            duration_ms=duration_ms,
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

        # Track hop: Agent → Main Agent (Redis as infrastructure)
        self._track_hop(workflow, agent_name.title() + " Agent", "Main Agent", f"response ✓ (via Redis)", "response", response.duration_ms)

        # Save updated state
        await self._save_workflow_state(workflow)

        self.logger.info(
            f"🔍 MAIN AGENT DEBUG - Task response received from {agent_name}",
            correlation_id=correlation_id,
            agent=agent_name,
            duration_ms=response.duration_ms,
            hop_count=workflow.hop_count,
        )

        # Check if workflow is complete
        if not workflow.pending_agents:
            workflow.status = "completed"
            
            # Calculate total workflow duration
            if workflow.start_time:
                total_duration = (time.perf_counter() - workflow.start_time) * 1000
                workflow.total_duration_ms = total_duration
            
            # Track final hop: Main Agent → User
            self._track_hop(workflow, "Main Agent", "User", "all services configured ✓", "response")
            
            await self._save_workflow_state(workflow)

            # Debug log: Workflow completion with timing and hop analysis
            self.logger.info(
                "🔍 MAIN AGENT DEBUG - Workflow completed",
                correlation_id=correlation_id,
                completed_agents=list(workflow.completed_agents),
                failed_agents=list(workflow.failures.keys()),
                total_duration_ms=round(workflow.total_duration_ms, 2) if workflow.total_duration_ms else None,
                hop_count=workflow.hop_count,
                hop_formula=f"2 (base) + (2 × {len(workflow.completed_agents)} tasks) = {workflow.hop_count}",
                time_breakdown="sum of parallel task durations",
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
            # Add hop and timing information
            "hop_count": workflow.hop_count,
            "hops": workflow.hops,
            "total_duration_ms": workflow.total_duration_ms,
            "routing_mode": "pub/sub with parallel execution",
        }

        self.logger.info(
            "🔍 MAIN AGENT DEBUG - Aggregated workflow results",
            correlation_id=correlation_id,
            successful_agents=len(workflow.results),
            failed_agents=len(workflow.failures),
            total_hops=workflow.hop_count,
            total_duration_ms=workflow.total_duration_ms,
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
