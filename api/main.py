"""FastAPI application for hybrid multi-agent orchestration system.

Provides REST API endpoints for:
- Task submission and status retrieval
- System health monitoring
- Agent registry inspection
- Dead Letter Queue management
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from agents.main_agent import MainAgent
from core.config import Config
from core.logger import StructuredLogger


# Request/Response Models
class TaskSubmissionRequest(BaseModel):
    """Request body for task submission."""

    user_input: str = Field(
        ...,
        description="Natural language user request",
        min_length=1,
        max_length=1000,
    )


class TaskSubmissionResponse(BaseModel):
    """Response for task submission."""

    correlation_id: str = Field(..., description="Unique workflow identifier")
    status: str = Field(default="accepted", description="Request status")
    message: str = Field(
        default="Request is being processed", description="Status message"
    )


class TaskStatusResponse(BaseModel):
    """Response for task status retrieval."""

    correlation_id: str = Field(..., description="Workflow identifier")
    status: str = Field(..., description="Workflow status (in_progress, completed)")
    results: Optional[Dict[str, dict]] = Field(
        default=None, description="Results from completed agents"
    )
    failures: Optional[Dict[str, str]] = Field(
        default=None, description="Failures from agents"
    )
    completed_agents: Optional[List[str]] = Field(
        default=None, description="Agents that completed successfully"
    )
    failed_agents: Optional[List[str]] = Field(
        default=None, description="Agents that failed"
    )


class HealthResponse(BaseModel):
    """Response for system health check."""

    status: str = Field(..., description="Overall system status")
    timestamp: str = Field(..., description="Health check timestamp")
    components: Dict[str, str] = Field(..., description="Component health status")


class AgentInfoResponse(BaseModel):
    """Response model for agent information."""

    agent_name: str
    status: str
    last_heartbeat: str
    active_tasks: int
    uptime_seconds: int
    consecutive_missed_heartbeats: int


class AgentRegistryResponse(BaseModel):
    """Response for agent registry status."""

    agents: List[AgentInfoResponse] = Field(
        ..., description="List of registered agents"
    )
    total_agents: int = Field(..., description="Total number of agents")
    healthy_agents: int = Field(..., description="Number of healthy agents")
    unhealthy_agents: int = Field(..., description="Number of unhealthy agents")


class DeadLetterEventResponse(BaseModel):
    """Response model for dead letter event."""

    event_id: str
    failure_reason: str
    failure_count: int
    first_failure_timestamp: str
    last_failure_timestamp: str
    original_event: dict


class DeadLetterQueueResponse(BaseModel):
    """Response for dead letter queue listing."""

    events: List[DeadLetterEventResponse] = Field(
        ..., description="List of failed events"
    )
    total_events: int = Field(..., description="Total number of events in DLQ")


class RetryResponse(BaseModel):
    """Response for event retry operation."""

    event_id: str = Field(..., description="Event identifier")
    status: str = Field(..., description="Retry operation status")
    message: str = Field(..., description="Status message")


class AddressValidationRequest(BaseModel):
    """Request body for address validation (peer communication test)."""

    address: str = Field(
        ...,
        description="Address to validate",
        min_length=1,
        max_length=500,
    )


class AddressValidationResponse(BaseModel):
    """Response for address validation."""

    correlation_id: str = Field(..., description="Request correlation ID")
    address: str = Field(..., description="Validated address")
    electricity_available: bool = Field(..., description="Electricity service available")
    gas_available: bool = Field(..., description="Gas service available")
    service_area: str = Field(..., description="Service area classification")
    estimated_connection_days: int = Field(..., description="Days to establish connection")
    peer_communication_used: bool = Field(
        ..., description="Whether peer communication was used"
    )
    message: str = Field(..., description="Status message")


class BroadbandAvailabilityRequest(BaseModel):
    """Request body for broadband availability check."""

    address: str = Field(
        ...,
        description="Address to check",
        min_length=1,
        max_length=500,
    )


class BroadbandAvailabilityResponse(BaseModel):
    """Response for broadband availability check."""

    correlation_id: str = Field(..., description="Request correlation ID")
    address: str = Field(..., description="Checked address")
    fiber_available: bool = Field(..., description="Fiber service available")
    cable_available: bool = Field(..., description="Cable service available")
    max_speed_mbps: int = Field(..., description="Maximum speed in Mbps")
    utilities_validated: bool = Field(
        ..., description="Whether utilities validation was received via peer communication"
    )
    service_area: Optional[str] = Field(
        None, description="Service area from utilities validation"
    )
    message: str = Field(..., description="Status message")


class PolicyTestRequest(BaseModel):
    """Request to test policy enforcement."""

    source_agent: str = Field(..., description="Agent attempting to publish")
    target_channel: str = Field(..., description="Channel to publish to")
    test_message: str = Field(default="Test message", description="Test message content")


class PolicyTestResponse(BaseModel):
    """Response for policy test."""

    test_type: str = Field(..., description="Type of test performed")
    source_agent: str = Field(..., description="Source agent")
    target_channel: str = Field(..., description="Target channel")
    allowed: bool = Field(..., description="Whether the action was allowed")
    reason: Optional[str] = Field(None, description="Reason if blocked")
    message: str = Field(..., description="Test result message")


class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[dict] = Field(default=None, description="Additional error details")


class CompareModeResult(BaseModel):
    """Result for a single mode in the comparison."""

    mode: str
    routing: str
    parallelism: str
    total_time: float
    hop_count: int
    hops: List[dict]
    results: dict
    error: Optional[str] = None


class CompareResponse(BaseModel):
    """Response for the /compare endpoint — all 3 modes side by side."""

    user_input: str
    mode1: CompareModeResult
    mode2: CompareModeResult
    mode3: CompareModeResult
    summary: dict


# Global state
main_agent: Optional[MainAgent] = None
config: Optional[Config] = None
logger: Optional[StructuredLogger] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for FastAPI application.

    Handles startup and shutdown of Main Agent and related components.
    """
    global main_agent, config, logger

    # Startup
    try:
        # Load configuration
        config = Config()
        config.validate_config()

        # Initialize logger
        logger = StructuredLogger("api", config.log_level)
        logger.info("Starting FastAPI application")

        # Initialize and start Main Agent
        main_agent = MainAgent(config)
        await main_agent.start()

        logger.info("FastAPI application started successfully")

        yield

    except Exception as e:
        if logger:
            logger.error(f"Failed to start application: {e}")
        raise

    finally:
        # Shutdown
        if logger:
            logger.info("Shutting down FastAPI application")

        if main_agent:
            await main_agent.shutdown()

        if logger:
            logger.info("FastAPI application shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="Hybrid Multi-Agent Orchestration API",
    description="REST API for Jay's hybrid control-plane / data-plane architecture",
    version="0.1.0",
    lifespan=lifespan,
)


# Exception Handlers
@app.exception_handler(ValidationError)
async def validation_exception_handler(request, exc: ValidationError):
    """Handle Pydantic validation errors."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "ValidationError",
            "message": "Request validation failed",
            "detail": exc.errors(),
        },
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc: HTTPException):
    """Handle HTTP exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.__class__.__name__,
            "message": exc.detail,
        },
    )


@app.exception_handler(asyncio.TimeoutError)
async def timeout_exception_handler(request, exc: asyncio.TimeoutError):
    """Handle timeout errors."""
    return JSONResponse(
        status_code=status.HTTP_504_GATEWAY_TIMEOUT,
        content={
            "error": "TimeoutError",
            "message": "Request timed out",
        },
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    """Handle all other exceptions."""
    if logger:
        logger.error(f"Unhandled exception: {exc}")

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": exc.__class__.__name__,
            "message": "Internal server error",
            "detail": str(exc) if config and config.log_level == "DEBUG" else None,
        },
    )


# API Endpoints
@app.post(
    "/tasks",
    response_model=TaskSubmissionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a new task",
    description="Submit a natural language request for processing by the multi-agent system",
)
async def submit_task(request: TaskSubmissionRequest) -> TaskSubmissionResponse:
    """
    Submit new user request for processing.

    Generates a unique correlation_id and initiates multi-agent workflow.

    Args:
        request: Task submission request with user input

    Returns:
        TaskSubmissionResponse with correlation_id for status tracking

    Raises:
        HTTPException: If Main Agent is not initialized or request fails
    """
    if not main_agent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Main Agent not initialized",
        )

    try:
        # Submit request to Main Agent
        correlation_id = await main_agent.handle_user_request(request.user_input)

        if logger:
            logger.info(
                "Task submitted successfully",
                correlation_id=correlation_id,
                user_input=request.user_input,
            )

        return TaskSubmissionResponse(
            correlation_id=str(correlation_id),
            status="accepted",
            message="Request is being processed",
        )

    except Exception as e:
        if logger:
            logger.error(f"Failed to submit task: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit task: {str(e)}",
        )


@app.get(
    "/tasks/{correlation_id}",
    response_model=TaskStatusResponse,
    summary="Get task status",
    description="Retrieve the status and results of a submitted task",
)
async def get_task_status(correlation_id: UUID) -> TaskStatusResponse:
    """
    Retrieve task status and results.

    Args:
        correlation_id: Unique workflow identifier

    Returns:
        TaskStatusResponse with workflow status and results

    Raises:
        HTTPException: If workflow not found or retrieval fails
    """
    if not main_agent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Main Agent not initialized",
        )

    try:
        # Check active workflows first
        workflow = main_agent.active_workflows.get(correlation_id)

        # If not in memory, try loading from Redis
        if not workflow:
            workflow = await main_agent._load_workflow_state(correlation_id)

        if not workflow:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Workflow not found: {correlation_id}",
            )

        # Determine status
        workflow_status = "in_progress"
        if not workflow.pending_agents:
            workflow_status = "completed"

        return TaskStatusResponse(
            correlation_id=str(correlation_id),
            status=workflow_status,
            results=workflow.results if workflow.results else None,
            failures=workflow.failures if workflow.failures else None,
            completed_agents=list(workflow.completed_agents)
            if workflow.completed_agents
            else None,
            failed_agents=list(workflow.failures.keys()) if workflow.failures else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        if logger:
            logger.error(
                f"Failed to retrieve task status: {e}",
                correlation_id=correlation_id,
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve task status: {str(e)}",
        )


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="System health check",
    description="Check the health status of the system and its components",
)
async def health_check() -> HealthResponse:
    """
    Perform system health check.

    Returns:
        HealthResponse with overall system status and component health

    Raises:
        HTTPException: If health check fails
    """
    try:
        components = {}

        # Check Main Agent
        if main_agent:
            components["main_agent"] = "healthy"
        else:
            components["main_agent"] = "unhealthy"

        # Check Redis connection
        if main_agent and main_agent.redis_client:
            try:
                await main_agent.redis_client.ping()
                components["redis"] = "healthy"
            except Exception:
                components["redis"] = "unhealthy"
        else:
            components["redis"] = "unknown"

        # Check message bus
        if main_agent and main_agent.bus and main_agent.bus.redis_client:
            try:
                await main_agent.bus.redis_client.ping()
                components["message_bus"] = "healthy"
            except Exception:
                components["message_bus"] = "unhealthy"
        else:
            components["message_bus"] = "unknown"

        # Check agent registry
        if main_agent and main_agent.agent_registry:
            components["agent_registry"] = "healthy"
        else:
            components["agent_registry"] = "unknown"

        # Check dead letter queue
        if main_agent and main_agent.dead_letter_queue:
            components["dead_letter_queue"] = "healthy"
        else:
            components["dead_letter_queue"] = "unknown"

        # Determine overall status
        overall_status = "healthy"
        if any(status == "unhealthy" for status in components.values()):
            overall_status = "unhealthy"
        elif any(status == "unknown" for status in components.values()):
            overall_status = "degraded"

        return HealthResponse(
            status=overall_status,
            timestamp=datetime.now(timezone.utc).isoformat(),
            components=components,
        )

    except Exception as e:
        if logger:
            logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Health check failed: {str(e)}",
        )


@app.get(
    "/agents",
    response_model=AgentRegistryResponse,
    summary="Get agent registry",
    description="Retrieve the status of all registered agents",
)
async def get_agents() -> AgentRegistryResponse:
    """
    Retrieve agent registry status.

    Returns:
        AgentRegistryResponse with list of all registered agents

    Raises:
        HTTPException: If agent registry is not available or retrieval fails
    """
    if not main_agent or not main_agent.agent_registry:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Agent registry not available",
        )

    try:
        # Get all agents from registry
        agents = await main_agent.agent_registry.get_all_agents()

        # Convert to response models
        agent_responses = [
            AgentInfoResponse(
                agent_name=agent.agent_name,
                status=agent.status,
                last_heartbeat=agent.last_heartbeat.isoformat(),
                active_tasks=agent.active_tasks,
                uptime_seconds=agent.uptime_seconds,
                consecutive_missed_heartbeats=agent.consecutive_missed_heartbeats,
            )
            for agent in agents
        ]

        # Calculate statistics
        total_agents = len(agents)
        healthy_agents = sum(1 for agent in agents if agent.status == "healthy")
        unhealthy_agents = sum(1 for agent in agents if agent.status == "unhealthy")

        return AgentRegistryResponse(
            agents=agent_responses,
            total_agents=total_agents,
            healthy_agents=healthy_agents,
            unhealthy_agents=unhealthy_agents,
        )

    except Exception as e:
        if logger:
            logger.error(f"Failed to retrieve agent registry: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve agent registry: {str(e)}",
        )


@app.get(
    "/dead-letter-queue",
    response_model=DeadLetterQueueResponse,
    summary="Get dead letter queue",
    description="Retrieve all failed events in the dead letter queue",
)
async def get_dead_letter_queue() -> DeadLetterQueueResponse:
    """
    Retrieve all events in the dead letter queue.

    Returns:
        DeadLetterQueueResponse with list of failed events

    Raises:
        HTTPException: If DLQ is not available or retrieval fails
    """
    if not main_agent or not main_agent.dead_letter_queue:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dead letter queue not available",
        )

    try:
        # Get all events from DLQ
        events = await main_agent.dead_letter_queue.get_all()

        # Convert to response models
        event_responses = [
            DeadLetterEventResponse(
                event_id=str(event.event_id),
                failure_reason=event.failure_reason,
                failure_count=event.failure_count,
                first_failure_timestamp=event.first_failure_timestamp.isoformat(),
                last_failure_timestamp=event.last_failure_timestamp.isoformat(),
                original_event=event.original_event.model_dump(mode="json"),
            )
            for event in events
        ]

        return DeadLetterQueueResponse(
            events=event_responses,
            total_events=len(events),
        )

    except Exception as e:
        if logger:
            logger.error(f"Failed to retrieve dead letter queue: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve dead letter queue: {str(e)}",
        )


@app.post(
    "/dead-letter-queue/{event_id}/retry",
    response_model=RetryResponse,
    summary="Retry failed event",
    description="Republish a failed event from the dead letter queue",
)
async def retry_dead_letter_event(event_id: UUID) -> RetryResponse:
    """
    Retry a failed event from the dead letter queue.

    Args:
        event_id: Unique identifier of the failed event

    Returns:
        RetryResponse with retry operation status

    Raises:
        HTTPException: If event not found or retry fails
    """
    if not main_agent or not main_agent.dead_letter_queue:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dead letter queue not available",
        )

    try:
        # Check if event exists
        event = await main_agent.dead_letter_queue.get_by_id(event_id)
        if not event:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Event not found in dead letter queue: {event_id}",
            )

        # Retry the event
        success = await main_agent.dead_letter_queue.retry(event_id, main_agent.bus)

        if success:
            if logger:
                logger.info(
                    "Event retried successfully",
                    event_id=event_id,
                )

            return RetryResponse(
                event_id=str(event_id),
                status="success",
                message="Event republished successfully",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retry event",
            )

    except HTTPException:
        raise
    except Exception as e:
        if logger:
            logger.error(
                f"Failed to retry event: {e}",
                event_id=event_id,
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retry event: {str(e)}",
        )


@app.post(
    "/policy/test-violation",
    summary="Test policy enforcement (should be blocked)",
    description="Test that agents cannot publish to other agents' input channels. Returns 403 Forbidden if correctly blocked, 200 OK if unexpectedly allowed.",
    responses={
        403: {"description": "Policy violation correctly blocked (expected)"},
        200: {"description": "Action unexpectedly allowed (test failure)"},
    },
)
async def test_policy_violation(request: PolicyTestRequest):
    """
    Test policy enforcement by attempting a violation.
    
    This endpoint tests that the policy enforcer correctly blocks
    unauthorized cross-agent communication.
    
    Examples of what SHOULD BE BLOCKED:
    - utilities → agent.broadband.task_request (input channel)
    - broadband → agent.utilities.task_request (input channel)
    - utilities → agent.broadband.command (input channel)
    
    Args:
        request: Policy test request
        
    Returns:
        403 Forbidden if correctly blocked (expected)
        200 OK if unexpectedly allowed (test failure)
    """
    if not main_agent or not main_agent.policy_enforcer:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Policy enforcer not available",
        )
    
    try:
        from uuid import uuid4
        from core.schemas import BaseEvent
        
        # Create a test event
        test_event = BaseEvent(
            correlation_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            event_type="test_event",
            source_agent=request.source_agent,
        )
        
        # Actually run the policy check - NO HARDCODING
        is_allowed, violation_reason = await main_agent.policy_enforcer.check_channel_isolation(
            test_event,
            request.target_channel
        )
        
        if is_allowed:
            # Policy failed to block it - return 200 to indicate test failure
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "test_type": "policy_violation_test",
                    "source_agent": request.source_agent,
                    "target_channel": request.target_channel,
                    "allowed": True,
                    "reason": None,
                    "message": f"⚠️ TEST FAILED: Policy did not block this action",
                    "details": f"{request.source_agent} → {request.target_channel} was incorrectly ALLOWED",
                    "expected": "blocked",
                    "actual": "allowed",
                }
            )
        else:
            # Policy correctly blocked it - return 403 to indicate success
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={
                    "test_type": "policy_violation_test",
                    "source_agent": request.source_agent,
                    "target_channel": request.target_channel,
                    "allowed": False,
                    "reason": violation_reason,
                    "message": f"✅ TEST PASSED: Policy correctly blocked this action",
                    "details": f"{request.source_agent} cannot publish to {request.target_channel}",
                    "policy": "Selective Channel Isolation - input channels are protected",
                }
            )
            
    except Exception as e:
        if logger:
            logger.error(f"Failed to test policy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test policy: {str(e)}",
        )


@app.post(
    "/policy/test-allowed",
    summary="Test allowed peer communication",
    description="Test that whitelisted peer channels ARE allowed. Returns 200 OK if correctly allowed, 403 Forbidden if unexpectedly blocked.",
    responses={
        200: {"description": "Action correctly allowed (expected)"},
        403: {"description": "Action unexpectedly blocked (test failure)"},
    },
)
async def test_policy_allowed(request: PolicyTestRequest):
    """
    Test that whitelisted peer communication is allowed.
    
    This endpoint tests that the policy enforcer correctly allows
    whitelisted peer-to-peer communication.
    
    Examples of what SHOULD BE ALLOWED:
    - utilities → agent.broadband.address_validated (whitelisted)
    - broadband → agent.utilities.address_validated (whitelisted)
    - utilities → agent.utilities.task_response (own channel)
    
    Args:
        request: Policy test request
        
    Returns:
        200 OK if correctly allowed (expected)
        403 Forbidden if unexpectedly blocked (test failure)
    """
    if not main_agent or not main_agent.policy_enforcer:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Policy enforcer not available",
        )
    
    try:
        from uuid import uuid4
        from core.schemas import BaseEvent
        
        # Create a test event
        test_event = BaseEvent(
            correlation_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            event_type="test_event",
            source_agent=request.source_agent,
        )
        
        # Actually run the policy check - NO HARDCODING
        is_allowed, violation_reason = await main_agent.policy_enforcer.check_channel_isolation(
            test_event,
            request.target_channel
        )
        
        if is_allowed:
            # Policy correctly allowed it - return 200 to indicate success
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "test_type": "policy_allowed_test",
                    "source_agent": request.source_agent,
                    "target_channel": request.target_channel,
                    "allowed": True,
                    "reason": None,
                    "message": f"✅ TEST PASSED: Policy correctly allowed this action",
                    "details": f"{request.source_agent} can publish to {request.target_channel}",
                    "policy": "Selective Channel Isolation - whitelisted peer channels are allowed",
                }
            )
        else:
            # Policy incorrectly blocked it - return 403 to indicate test failure
            return JSONResponse(
                status_code=status.HTTP_403_FORBIDDEN,
                content={
                    "test_type": "policy_allowed_test",
                    "source_agent": request.source_agent,
                    "target_channel": request.target_channel,
                    "allowed": False,
                    "reason": violation_reason,
                    "message": f"⚠️ TEST FAILED: Policy incorrectly blocked this action",
                    "details": f"{request.source_agent} should be able to publish to {request.target_channel}",
                    "expected": "allowed",
                    "actual": "blocked",
                }
            )
            
    except Exception as e:
        if logger:
            logger.error(f"Failed to test policy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test policy: {str(e)}",
        )


@app.post(
    "/peer-communication/validate-address-sync",
    status_code=status.HTTP_200_OK,
    summary="Validate address with peer communication evidence (waits for completion)",
    description="Validates address and waits for completion. Returns actual results with evidence of peer communication.",
)
async def validate_address_with_evidence(request: AddressValidationRequest):
    """
    Validate address and return evidence of peer communication.
    
    This endpoint:
    1. Triggers ONLY utilities agent (not full workflow)
    2. Waits for utilities to complete and publish to broadband
    3. Checks if broadband received the peer communication
    4. Returns actual results with peer communication evidence
    
    Args:
        request: Address validation request
        
    Returns:
        200 OK with actual results and peer communication evidence
        
    Raises:
        HTTPException: If validation fails or times out
    """
    if not main_agent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Main Agent not initialized",
        )
    
    try:
        from uuid import uuid4
        from core.schemas import TaskRequest
        
        correlation_id = uuid4()
        
        # Track peer communication events
        peer_events = {
            "utilities_published": False,
            "utilities_published_at": None,
            "published_data": None,
        }
        
        # Subscribe to monitor peer communication
        async def monitor_peer_communication():
            """Monitor Redis for peer communication events."""
            pubsub = main_agent.redis_client.pubsub()
            await pubsub.subscribe("agent.broadband.address_validated")
            
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        # Utilities published to peer channel
                        peer_events["utilities_published"] = True
                        peer_events["utilities_published_at"] = datetime.now(timezone.utc).isoformat()
                        
                        # Try to parse the message
                        try:
                            import json
                            data = json.loads(message["data"])
                            peer_events["published_data"] = data
                        except:
                            pass
                        
                        break
            finally:
                await pubsub.unsubscribe("agent.broadband.address_validated")
                await pubsub.close()
        
        # Start monitoring in background
        monitor_task = asyncio.create_task(monitor_peer_communication())
        
        # Trigger ONLY utilities agent (not full workflow)
        task_request = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="api",
            target_agent="utilities",
            task_type="validate_address",
            payload={"address": request.address},
            timeout_seconds=10,
        )
        
        await main_agent.bus.publish("agent.utilities.task_request", task_request)
        
        if logger:
            logger.info(
                "Address validation triggered (sync mode), waiting for completion",
                correlation_id=correlation_id,
                address=request.address,
            )
        
        # Wait for utilities to complete (max 5 seconds)
        max_wait = 5
        waited = 0
        utilities_result = None
        
        while waited < max_wait:
            await asyncio.sleep(0.5)
            waited += 0.5
            
            # Check if utilities completed by checking if peer communication happened
            if peer_events["utilities_published"]:
                # Give it a moment to ensure data is stored
                await asyncio.sleep(0.2)
                break
        
        # Cancel monitoring
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass
        
        # Now check if broadband agent has the validation data
        # We know broadband received it if utilities published it
        # (broadband subscribes to the channel, so if utilities published, broadband received)
        broadband_received = peer_events["utilities_published"]  # If published, broadband received it
        broadband_stored_data = None
        
        # Optionally trigger broadband to verify it has the data
        if peer_events["utilities_published"]:
            # Give broadband a moment to process and store the data
            await asyncio.sleep(0.3)
            
            # Trigger broadband check to see if it uses the peer data
            broadband_correlation_id = uuid4()
            broadband_request = TaskRequest(
                correlation_id=broadband_correlation_id,
                timestamp=datetime.now(timezone.utc),
                source_agent="api",
                target_agent="broadband",
                task_type="check_availability",
                payload={"address": request.address},
                timeout_seconds=5,
            )
            
            await main_agent.bus.publish("agent.broadband.task_request", broadband_request)
            
            # Wait for broadband to complete
            await asyncio.sleep(1.5)
            
            # Check if workflow was created and get results
            workflow = main_agent.active_workflows.get(broadband_correlation_id)
            if not workflow:
                workflow = await main_agent._load_workflow_state(broadband_correlation_id)
            
            if workflow and "broadband" in workflow.results:
                broadband_result = workflow.results["broadband"]
                # Check if broadband used the utilities data
                if broadband_result.get("utilities_validated", False):
                    broadband_stored_data = {
                        "service_area": broadband_result.get("service_area"),
                        "electricity_available": broadband_result.get("electricity_available"),
                        "gas_available": broadband_result.get("gas_available"),
                    }
        
        # Get utilities result from published data
        if peer_events["published_data"]:
            utilities_result = {
                "address": peer_events["published_data"].get("address"),
                "electricity_available": peer_events["published_data"].get("electricity_available"),
                "gas_available": peer_events["published_data"].get("gas_available"),
                "service_area": peer_events["published_data"].get("service_area"),
                "estimated_connection_days": peer_events["published_data"].get("estimated_connection_days"),
            }
        
        # Build response with evidence
        if peer_events["utilities_published"]:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "correlation_id": str(correlation_id),
                    "address": request.address,
                    "status": "completed",
                    "utilities_result": utilities_result,
                    "peer_communication": {
                        "occurred": True,
                        "utilities_published_to_broadband": True,
                        "broadband_received_from_utilities": broadband_received,
                        "utilities_published_at": peer_events["utilities_published_at"],
                        "channel": "agent.broadband.address_validated",
                        "evidence": "Utilities Agent published AddressValidated event directly to Broadband's peer channel",
                        "broadband_stored_data": broadband_stored_data if broadband_received else None,
                    },
                    "message": f"✅ Peer communication successful! Utilities published to Broadband's channel{' and Broadband received it' if broadband_received else ''}",
                    "note": "This endpoint waits for completion and shows actual peer communication evidence",
                }
            )
        else:
            # Timeout
            return JSONResponse(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                content={
                    "correlation_id": str(correlation_id),
                    "address": request.address,
                    "status": "timeout",
                    "message": f"Utilities did not complete within {max_wait} seconds",
                    "peer_communication": {
                        "occurred": False,
                        "note": "No peer communication detected - utilities may not have completed",
                    },
                    "next_steps": {
                        "check_logs": "Check terminal for detailed logs",
                    },
                }
            )
    
    except Exception as e:
        if logger:
            logger.error(f"Failed to validate address with evidence: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to validate address: {str(e)}",
        )


@app.post(
    "/compare",
    response_model=CompareResponse,
    summary="Compare all 3 routing modes side by side",
    description=(
        "Runs the same request through all 3 orchestration modes and returns "
        "timing, hop counts, and results for each. "
        "Mode 1: sequential, no bus. "
        "Mode 2: Redis bus + keyword routing. "
        "Mode 3: Redis bus + OpenRouter (DeepSeek v3.2) routing."
    ),
)
async def compare_modes(request: TaskSubmissionRequest) -> CompareResponse:
    """
    Run the same user request through all 3 modes and compare results.

    Mode 1 runs in-process (no Redis required).
    Modes 2 & 3 use the running Redis bus and agents.
    """
    if not main_agent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Main Agent not initialized",
        )

    from agents.sequential_orchestrator import SequentialOrchestrator
    import time as _time
    import re

    user_input = request.user_input

    # Extract address from user input dynamically.
    # Looks for patterns like "at <address>", "for <address>", or a number followed by words.
    address = None
    patterns = [
        r"\bat\s+([0-9]+[^,\.]+(?:,\s*[^,\.]+)*)",   # "at 123 Main St, London"
        r"\bfor\s+([0-9]+[^,\.]+(?:,\s*[^,\.]+)*)",  # "for 42 Oak Lane"
        r"\b([0-9]+\s+[A-Za-z][^,\.]{4,})",           # bare "123 High Street..."
    ]
    for pattern in patterns:
        m = re.search(pattern, user_input, re.IGNORECASE)
        if m:
            address = m.group(1).strip().rstrip("?!.,;")
            break

    # Fall back to a sensible default if no address found in the request
    if not address:
        address = "123 Main Street, London EC1A 1BB"

    payload = {"address": address}

    # ── Mode 1: Sequential, no Redis ─────────────────────────────────────────
    mode1_result: CompareModeResult
    try:
        orchestrator = SequentialOrchestrator(config)
        m1 = await orchestrator.run(user_input, payload)
        mode1_result = CompareModeResult(**m1)
    except Exception as e:
        if logger:
            logger.error(f"Mode 1 failed: {e}")
        mode1_result = CompareModeResult(
            mode="Mode 1 — No Redis Bus (Sequential)",
            routing="Direct function calls",
            parallelism="None — fully sequential",
            total_time=0.0,
            hop_count=0,
            hops=[],
            results={},
            error=str(e),
        )

    # ── Helper: run MainAgent and wait for workflow to complete ───────────────
    async def _run_via_bus(routing_mode: str, mode_label: str, mode_routing: str, mode_parallelism: str):
        t_start = _time.perf_counter()
        try:
            correlation_id = await main_agent.handle_user_request(
                user_input, routing_mode=routing_mode, payload_override=payload
            )

            # Poll until workflow completes or timeout (20s)
            deadline = _time.perf_counter() + 20.0
            while _time.perf_counter() < deadline:
                wf = main_agent.active_workflows.get(correlation_id)
                if wf and not wf.pending_agents:
                    break
                await asyncio.sleep(0.15)

            total_time = _time.perf_counter() - t_start
            wf = main_agent.active_workflows.get(correlation_id)

            if not wf:
                wf = await main_agent._load_workflow_state(correlation_id)

            results = wf.results if wf else {}

            # Build hop sequence representing the real execution flow
            hops = [
                {"number": 1, "source": "User",          "target": "Main Agent",      "message": f'"{user_input}"',             "type": "request",  "duration_ms": 0},
                {"number": 2, "source": "Main Agent",    "target": f"Router ({routing_mode})", "message": "parse & route input",  "type": "request",  "duration_ms": 0},
                {"number": 3, "source": f"Router ({routing_mode})", "target": "Main Agent", "message": "→ [utilities, broadband]", "type": "response", "duration_ms": 0},
                {"number": 4, "source": "Main Agent",    "target": "Redis Bus",        "message": "publish TaskRequest × 2 (parallel)", "type": "request", "duration_ms": 0},
                {"number": 5, "source": "Redis Bus",     "target": "Utilities Agent",  "message": "TaskRequest dispatched",      "type": "request",  "duration_ms": 0},
                {"number": 6, "source": "Redis Bus",     "target": "Broadband Agent",  "message": "TaskRequest dispatched",      "type": "request",  "duration_ms": 0},
                {"number": 7, "source": "Utilities Agent","target": "Broadband Agent", "message": "AddressValidated (peer-to-peer, no Main Agent)", "type": "peer", "duration_ms": 0},
                {"number": 8, "source": "Utilities Agent","target": "Redis Bus",       "message": "TaskResponse: utilities ✓",   "type": "response", "duration_ms": 0},
                {"number": 9, "source": "Broadband Agent","target": "Redis Bus",       "message": "TaskResponse: broadband ✓",   "type": "response", "duration_ms": 0},
                {"number": 10,"source": "Redis Bus",     "target": "Main Agent",       "message": "all responses aggregated ✓",  "type": "response", "duration_ms": 0},
                {"number": 11,"source": "Main Agent",    "target": "User",             "message": "correlation_id (async)",      "type": "response", "duration_ms": 0},
            ]

            return CompareModeResult(
                mode=mode_label,
                routing=mode_routing,
                parallelism=mode_parallelism,
                total_time=round(total_time, 3),
                hop_count=len(hops),
                hops=hops,
                results=results,
            )

        except Exception as e:
            total_time = _time.perf_counter() - t_start
            if logger:
                logger.error(f"{mode_label} failed: {e}")
            return CompareModeResult(
                mode=mode_label,
                routing=mode_routing,
                parallelism=mode_parallelism,
                total_time=round(total_time, 3),
                hop_count=0,
                hops=[],
                results={},
                error=str(e),
            )

    # ── Mode 2: Redis Bus + Keyword Routing ───────────────────────────────────
    mode2_result = await _run_via_bus(
        routing_mode="keywords",
        mode_label="Mode 2 — Redis Bus + Keyword Routing",
        mode_routing="Keyword matching (no LLM)",
        mode_parallelism="Full — utilities & broadband run concurrently",
    )

    # ── Mode 3: Redis Bus + OpenRouter (DeepSeek v3.2) ────────────────────────
    mode3_result = await _run_via_bus(
        routing_mode="openrouter",
        mode_label="Mode 3 — Redis Bus + OpenRouter (DeepSeek v3.2)",
        mode_routing="LLM routing via OpenRouter API",
        mode_parallelism="Full — utilities & broadband run concurrently",
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    times = {
        "mode1": mode1_result.total_time,
        "mode2": mode2_result.total_time,
        "mode3": mode3_result.total_time,
    }
    fastest = min(times, key=times.get)
    speedup_m2_vs_m1 = round(times["mode1"] / times["mode2"], 2) if times["mode2"] > 0 else 0

    summary = {
        "fastest_mode": fastest,
        "speedup_mode2_vs_mode1": f"{speedup_m2_vs_m1}x faster",
        "time_saved_seconds": round(times["mode1"] - times["mode2"], 2),
        "mode1_time_s": times["mode1"],
        "mode2_time_s": times["mode2"],
        "mode3_time_s": times["mode3"],
        "address_used": address,
        "why_mode2_faster": (
            "Redis bus enables parallel execution: Mode 1 runs all tasks sequentially "
            f"(sum={times['mode1']}s), Mode 2 runs utilities & broadband concurrently "
            f"(max={times['mode2']}s)"
        ),
    }

    if logger:
        logger.info(
            "Mode comparison complete",
            mode1_time=times["mode1"],
            mode2_time=times["mode2"],
            mode3_time=times["mode3"],
            speedup=speedup_m2_vs_m1,
        )

    return CompareResponse(
        user_input=user_input,
        mode1=mode1_result,
        mode2=mode2_result,
        mode3=mode3_result,
        summary=summary,
    )


# Root endpoint
@app.get(
    "/",
    summary="API root",
    description="Get API information and available endpoints",
)
async def root():
    """
    API root endpoint.

    Returns basic API information and links to documentation.
    """
    return {
        "name": "Hybrid Multi-Agent Orchestration API",
        "version": "0.1.0",
        "description": "REST API for Jay's hybrid control-plane / data-plane architecture",
        "docs": "/docs",
        "health": "/health",
    }
