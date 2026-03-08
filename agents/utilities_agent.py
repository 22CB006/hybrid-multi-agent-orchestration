"""Utilities Agent - Data Plane Agent for electricity and gas setup.

Handles utilities-specific tasks:
- validate_address: Check service availability at address
- setup_electricity: Initiate electricity service
- setup_gas: Initiate gas service
- get_quote: Retrieve pricing information
"""

import asyncio
from typing import Any

from agents.base_agent import BaseDataPlaneAgent
from core.config import Config
from core.schemas import TaskRequest


class UtilitiesAgent(BaseDataPlaneAgent):
    """Data plane agent handling electricity and gas setup tasks."""

    TASK_TYPES = ["validate_address", "setup_electricity", "setup_gas", "get_quote"]
    KEYWORDS = [
        # Electricity
        "electricity", "electric", "electrical", "elec",
        "power", "power supply", "power connection",
        "current", "voltage", "wiring",
        "meter", "electric meter", "power meter",
        "eb", "eb connection", "electricity board",
        "tneb", "tangedco",  # Tamil Nadu electricity board
        "light", "light connection", "lights",
        # Gas
        "gas", "gas connection", "gas line", "gas pipe",
        "lpg", "cooking gas", "gas cylinder", "gas stove",
        "piped gas", "png", "natural gas", "cng",
        "indane", "hp gas", "bharat gas",
        # General utilities
        "utilities", "utility", "utility services",
        "energy", "energy connection",
        "bill", "electricity bill", "gas bill", "utility bill",
        "tariff", "rate", "unit rate",
        # Actions
        "new connection", "reconnect", "disconnect",
        "transfer connection", "name transfer",
    ]
    DESCRIPTION = "Handles electricity and gas utility setup, address validation, service quotes, and new connections"

    def __init__(self, config: Config):
        """Initialize Utilities Agent.

        Args:
            config: System configuration
        """
        super().__init__(agent_name="utilities", config=config)

    async def execute_task(self, request: TaskRequest) -> dict:
        """Execute utilities-specific task logic.

        Handles task types:
        - validate_address: Checks service availability at address
        - setup_electricity: Initiates electricity service
        - setup_gas: Initiates gas service
        - get_quote: Retrieves pricing information

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
        if task_type == "validate_address":
            return await self._validate_address(payload)
        elif task_type == "setup_electricity":
            return await self._setup_electricity(payload)
        elif task_type == "setup_gas":
            return await self._setup_gas(payload)
        elif task_type == "get_quote":
            return await self._get_quote(payload)
        else:
            raise ValueError(f"Unknown task type: {task_type}")

    async def _validate_address(self, payload: dict) -> dict:
        """Validate service availability at address.

        Args:
            payload: Must contain 'address' field

        Returns:
            Validation result with availability status
        """
        address = payload.get("address")
        if not address:
            raise ValueError("Address is required for validation")

        # Simulate address validation logic
        # In production, this would call external APIs
        await asyncio.sleep(0.1)  # Simulate API call

        return {
            "address": address,
            "electricity_available": True,
            "gas_available": True,
            "service_area": "metro",
            "estimated_connection_days": 3,
        }

    async def _setup_electricity(self, payload: dict) -> dict:
        """Initiate electricity service setup.

        Args:
            payload: Must contain 'address' and optionally 'plan_id', 'start_date'

        Returns:
            Setup confirmation with service details
        """
        address = payload.get("address")
        plan_id = payload.get("plan_id", "standard")
        start_date = payload.get("start_date")

        if not address:
            raise ValueError("Address is required for electricity setup")

        # Simulate electricity setup logic
        # In production, this would call utility provider APIs
        await asyncio.sleep(0.2)  # Simulate API call

        return {
            "service_type": "electricity",
            "address": address,
            "plan_id": plan_id,
            "start_date": start_date,
            "account_number": f"ELEC-{hash(address) % 100000:05d}",
            "status": "pending_activation",
            "estimated_activation_date": start_date or "within_3_business_days",
        }

    async def _setup_gas(self, payload: dict) -> dict:
        """Initiate gas service setup.

        Args:
            payload: Must contain 'address' and optionally 'plan_id', 'start_date'

        Returns:
            Setup confirmation with service details
        """
        address = payload.get("address")
        plan_id = payload.get("plan_id", "standard")
        start_date = payload.get("start_date")

        if not address:
            raise ValueError("Address is required for gas setup")

        # Simulate gas setup logic
        # In production, this would call utility provider APIs
        await asyncio.sleep(0.2)  # Simulate API call

        return {
            "service_type": "gas",
            "address": address,
            "plan_id": plan_id,
            "start_date": start_date,
            "account_number": f"GAS-{hash(address) % 100000:05d}",
            "status": "pending_activation",
            "estimated_activation_date": start_date or "within_3_business_days",
            "safety_inspection_required": True,
        }

    async def _get_quote(self, payload: dict) -> dict:
        """Retrieve pricing information for utilities services.

        Args:
            payload: Must contain 'address' and optionally 'services' list

        Returns:
            Pricing quotes for requested services
        """
        address = payload.get("address")
        services = payload.get("services", ["electricity", "gas"])

        if not address:
            raise ValueError("Address is required for quote")

        # Simulate quote retrieval logic
        # In production, this would call pricing APIs
        await asyncio.sleep(0.15)  # Simulate API call

        quotes = []
        if "electricity" in services:
            quotes.append(
                {
                    "service_type": "electricity",
                    "plan_name": "Standard Residential",
                    "monthly_base_fee": 15.00,
                    "rate_per_kwh": 0.12,
                    "estimated_monthly_cost": 85.00,
                }
            )

        if "gas" in services:
            quotes.append(
                {
                    "service_type": "gas",
                    "plan_name": "Standard Residential",
                    "monthly_base_fee": 12.00,
                    "rate_per_therm": 0.95,
                    "estimated_monthly_cost": 65.00,
                }
            )

        return {
            "address": address,
            "quotes": quotes,
            "total_estimated_monthly": sum(
                q.get("estimated_monthly_cost", 0) for q in quotes
            ),
            "quote_valid_until": "30_days",
        }


if __name__ == "__main__":
    """Run Utilities Agent as standalone service."""
    import asyncio

    async def main():
        """Initialize and start Utilities Agent."""
        config = Config()
        agent = UtilitiesAgent(config)
        await agent.start()

        # Keep running until interrupted
        try:
            await asyncio.Event().wait()  # Blocks forever
        except (KeyboardInterrupt, asyncio.CancelledError):
            await agent.shutdown()

    asyncio.run(main())
