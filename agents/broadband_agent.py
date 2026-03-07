"""Broadband Agent - Data Plane Agent for internet and broadband setup.

Handles broadband-specific tasks:
- check_availability: Verify broadband coverage at address
- setup_internet: Initiate internet service
- get_plans: Retrieve available broadband plans
- schedule_installation: Book installation appointment
"""

import asyncio
from datetime import datetime, timedelta

from agents.base_agent import BaseDataPlaneAgent
from core.config import Config
from core.schemas import TaskRequest


class BroadbandAgent(BaseDataPlaneAgent):
    """Data plane agent handling internet and broadband setup tasks."""

    def __init__(self, config: Config):
        """Initialize Broadband Agent.

        Args:
            config: System configuration
        """
        super().__init__(agent_name="broadband", config=config)

    async def execute_task(self, request: TaskRequest) -> dict:
        """Execute broadband-specific task logic.

        Handles task types:
        - check_availability: Verifies broadband coverage
        - setup_internet: Initiates internet service
        - get_plans: Retrieves available plans
        - schedule_installation: Books installation appointment

        Args:
            request: Task request with task_type and payload

        Returns:
            Task execution result as dictionary

        Raises:
            ValueError: If task_type is unknown
            TimeoutError: If task exceeds timeout_seconds
            Exception: On task execution failure
        """
        task_type = request.task_type
        payload = request.payload
        timeout_seconds = request.timeout_seconds

        # Execute task with timeout
        try:
            result = await asyncio.wait_for(
                self._execute_task_internal(task_type, payload), timeout=timeout_seconds
            )
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Task {task_type} exceeded timeout of {timeout_seconds} seconds"
            )

    async def _execute_task_internal(self, task_type: str, payload: dict) -> dict:
        """Internal task execution logic without timeout wrapper.

        Args:
            task_type: Type of task to perform
            payload: Task-specific parameters

        Returns:
            Task execution result

        Raises:
            ValueError: If task_type is unknown
        """
        if task_type == "check_availability":
            return await self._check_availability(payload)
        elif task_type == "setup_internet":
            return await self._setup_internet(payload)
        elif task_type == "get_plans":
            return await self._get_plans(payload)
        elif task_type == "schedule_installation":
            return await self._schedule_installation(payload)
        else:
            raise ValueError(f"Unknown task type: {task_type}")

    async def _check_availability(self, payload: dict) -> dict:
        """Verify broadband coverage at address.

        Args:
            payload: Must contain 'address' field

        Returns:
            Availability result with coverage details
        """
        address = payload.get("address")
        if not address:
            raise ValueError("Address is required for availability check")

        # Simulate availability check logic
        # In production, this would call ISP coverage APIs
        await asyncio.sleep(0.1)  # Simulate API call

        return {
            "address": address,
            "fiber_available": True,
            "cable_available": True,
            "dsl_available": False,
            "max_speed_mbps": 1000,
            "providers": ["FastNet", "CableLink", "FiberPro"],
            "installation_required": True,
        }

    async def _setup_internet(self, payload: dict) -> dict:
        """Initiate internet service setup.

        Args:
            payload: Must contain 'address' and optionally 'plan_id', 'start_date', 'provider'

        Returns:
            Setup confirmation with service details
        """
        address = payload.get("address")
        plan_id = payload.get("plan_id", "fiber_500")
        start_date = payload.get("start_date")
        provider = payload.get("provider", "FastNet")

        if not address:
            raise ValueError("Address is required for internet setup")

        # Simulate internet setup logic
        # In production, this would call ISP provisioning APIs
        await asyncio.sleep(0.2)  # Simulate API call

        return {
            "service_type": "internet",
            "address": address,
            "provider": provider,
            "plan_id": plan_id,
            "start_date": start_date,
            "account_number": f"BB-{hash(address) % 100000:05d}",
            "status": "pending_installation",
            "connection_type": "fiber" if "fiber" in plan_id else "cable",
            "installation_required": True,
            "estimated_installation_date": start_date or "within_5_business_days",
        }

    async def _get_plans(self, payload: dict) -> dict:
        """Retrieve available broadband plans.

        Args:
            payload: Must contain 'address' and optionally 'connection_type'

        Returns:
            List of available plans with pricing
        """
        address = payload.get("address")
        connection_type = payload.get("connection_type", "all")

        if not address:
            raise ValueError("Address is required for plan retrieval")

        # Simulate plan retrieval logic
        # In production, this would call ISP pricing APIs
        await asyncio.sleep(0.15)  # Simulate API call

        plans = []

        if connection_type in ["all", "fiber"]:
            plans.extend(
                [
                    {
                        "plan_id": "fiber_500",
                        "plan_name": "Fiber 500",
                        "connection_type": "fiber",
                        "download_speed_mbps": 500,
                        "upload_speed_mbps": 500,
                        "monthly_cost": 79.99,
                        "installation_fee": 99.00,
                        "contract_months": 12,
                    },
                    {
                        "plan_id": "fiber_1000",
                        "plan_name": "Fiber Gigabit",
                        "connection_type": "fiber",
                        "download_speed_mbps": 1000,
                        "upload_speed_mbps": 1000,
                        "monthly_cost": 99.99,
                        "installation_fee": 99.00,
                        "contract_months": 12,
                    },
                ]
            )

        if connection_type in ["all", "cable"]:
            plans.extend(
                [
                    {
                        "plan_id": "cable_300",
                        "plan_name": "Cable 300",
                        "connection_type": "cable",
                        "download_speed_mbps": 300,
                        "upload_speed_mbps": 30,
                        "monthly_cost": 59.99,
                        "installation_fee": 49.00,
                        "contract_months": 12,
                    },
                    {
                        "plan_id": "cable_600",
                        "plan_name": "Cable 600",
                        "connection_type": "cable",
                        "download_speed_mbps": 600,
                        "upload_speed_mbps": 60,
                        "monthly_cost": 79.99,
                        "installation_fee": 49.00,
                        "contract_months": 12,
                    },
                ]
            )

        return {
            "address": address,
            "plans": plans,
            "plans_count": len(plans),
            "quote_valid_until": "30_days",
        }

    async def _schedule_installation(self, payload: dict) -> dict:
        """Book installation appointment.

        Args:
            payload: Must contain 'address', 'account_number' and optionally 'preferred_date', 'time_slot'

        Returns:
            Installation appointment confirmation
        """
        address = payload.get("address")
        account_number = payload.get("account_number")
        preferred_date = payload.get("preferred_date")
        time_slot = payload.get("time_slot", "morning")

        if not address:
            raise ValueError("Address is required for installation scheduling")
        if not account_number:
            raise ValueError("Account number is required for installation scheduling")

        # Simulate installation scheduling logic
        # In production, this would call ISP scheduling APIs
        await asyncio.sleep(0.2)  # Simulate API call

        # Calculate installation date (5 business days from now if not specified)
        if preferred_date:
            installation_date = preferred_date
        else:
            installation_date = (datetime.now() + timedelta(days=5)).strftime(
                "%Y-%m-%d"
            )

        time_windows = {
            "morning": "8:00 AM - 12:00 PM",
            "afternoon": "12:00 PM - 5:00 PM",
            "evening": "5:00 PM - 8:00 PM",
        }

        return {
            "address": address,
            "account_number": account_number,
            "installation_date": installation_date,
            "time_slot": time_slot,
            "time_window": time_windows.get(time_slot, time_windows["morning"]),
            "technician_assigned": True,
            "confirmation_number": f"INST-{hash(account_number) % 100000:05d}",
            "status": "confirmed",
            "estimated_duration_hours": 2,
            "customer_presence_required": True,
        }
