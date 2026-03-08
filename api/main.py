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


class ErrorResponse(BaseModel):
    """Standard error response."""

    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[dict] = Field(default=None, description="Additional error details")


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
