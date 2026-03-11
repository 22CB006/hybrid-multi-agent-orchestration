"""Event schemas for hybrid multi-agent communication.

All events extend BaseEvent and use Pydantic v2 for validation.
Events are transmitted via Redis Pub/Sub as JSON.
"""

from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_serializer


class BaseEvent(BaseModel):
    """Base class for all events in the system."""

    correlation_id: UUID = Field(
        ..., description="Unique ID tracking request across agents"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event creation time",
    )
    event_type: str = Field(..., description="Event category identifier")
    source_agent: str = Field(..., description="Agent that created this event")

    @field_serializer("timestamp")
    def serialize_timestamp(self, dt: datetime, _info):
        """Serialize datetime to ISO format."""
        return dt.isoformat()

    @field_serializer("correlation_id")
    def serialize_uuid(self, uuid_val: UUID, _info):
        """Serialize UUID to string."""
        return str(uuid_val)


class TaskRequest(BaseEvent):
    """Request for an agent to perform a task."""

    event_type: Literal["task_request"] = "task_request"
    target_agent: str = Field(..., description="Agent that should handle this task")
    task_type: str = Field(..., description="Type of task to perform")
    payload: dict = Field(..., description="Task-specific parameters")
    timeout_seconds: float = Field(default=30.0, description="Maximum execution time in seconds")


class TaskResponse(BaseEvent):
    """Successful task completion response."""

    event_type: Literal["task_response"] = "task_response"
    task_id: UUID = Field(..., description="ID of completed task")
    result: dict = Field(..., description="Task execution results")
    duration_ms: int = Field(..., description="Task execution duration")


class TaskFailure(BaseEvent):
    """Task execution failure notification."""

    event_type: Literal["task_failure"] = "task_failure"
    task_id: UUID = Field(..., description="ID of failed task")
    error_type: str = Field(..., description="Error category")
    error_message: str = Field(..., description="Human-readable error description")
    retry_count: int = Field(default=0, description="Number of retry attempts")
    is_retryable: bool = Field(..., description="Whether task can be retried")


class HealthCheck(BaseEvent):
    """Agent health status heartbeat."""

    event_type: Literal["health_check"] = "health_check"
    agent_status: Literal["healthy", "degraded", "unhealthy"] = Field(
        ..., description="Current agent status"
    )
    active_tasks: int = Field(
        default=0, description="Number of tasks currently processing"
    )
    uptime_seconds: int = Field(..., description="Time since agent started")


class AddressValidated(BaseEvent):
    """Address validation result shared between agents.
    
    This event enables direct peer-to-peer communication:
    - Utilities Agent validates address and publishes this event
    - Broadband Agent subscribes and receives validation result directly
    - No routing through Main Agent required
    """

    event_type: Literal["address_validated"] = "address_validated"
    address: str = Field(..., description="Validated address")
    electricity_available: bool = Field(..., description="Electricity service available")
    gas_available: bool = Field(..., description="Gas service available")
    service_area: str = Field(..., description="Service area classification")
    estimated_connection_days: int = Field(..., description="Days to establish connection")
    validation_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When validation was performed"
    )


class PolicyViolation(BaseEvent):
    """Policy enforcement violation detected by Main Agent."""

    event_type: Literal["policy_violation"] = "policy_violation"
    violation_type: str = Field(..., description="Type of policy violated")
    violating_agent: str = Field(..., description="Agent that violated policy")
    violation_details: dict = Field(..., description="Additional context")
    action_taken: str = Field(..., description="Enforcement action applied")



