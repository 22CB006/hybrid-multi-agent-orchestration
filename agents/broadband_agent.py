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

    TASK_TYPES = ["check_availability", "setup_internet", "get_plans", "schedule_installation"]
    KEYWORDS = [
        # Internet
        "internet", "internet connection", "net", "net connection",
        "online", "go online", "get online",
        # Broadband
        "broadband", "broad band", "bb",
        # WiFi
        "wifi", "wi-fi", "wi fi", "wireless",
        "router", "modem", "hotspot",
        # Fiber
        "fiber", "fibre", "fiber optic", "fibre optic",
        "ftth", "fttx", "optical fiber",
        # Connection types
        "cable", "cable internet", "dsl", "adsl", "vdsl",
        "leased line", "dedicated line",
        # Speed
        "mbps", "gbps", "speed", "bandwidth", "high speed",
        "fast internet", "slow internet", "speed test",
        # ISP / Providers
        "isp", "provider", "service provider",
        "jio", "jio fiber", "jiofiber",
        "airtel", "airtel fiber", "airtel broadband",
        "act", "act fibernet", "act broadband",
        "bsnl", "bsnl broadband", "bsnl fiber",
        "hathway", "tikona", "excitel", "tata play",
        # Plans
        "plan", "plans", "data plan", "unlimited",
        "ott", "streaming", "netflix", "hotstar",
        # Installation
        "installation", "install", "technician",
        "setup box", "set top box",
    ]
    DESCRIPTION = "Handles internet and broadband setup, availability checks, plan comparison, and installation scheduling"

    def __init__(self, config: Config):
        """Initialize Broadband Agent.

        Args:
            config: System configuration
        """
        super().__init__(agent_name="broadband", config=config)
        
        # Store validated addresses received from Utilities Agent
        self.validated_addresses = {}

    async def start(self) -> None:
        """Start Broadband Agent with additional peer communication subscriptions."""
        # Call parent start method
        await super().start()
        
        # Subscribe to address validation events from Utilities Agent (peer communication)
        await self.bus.subscribe(
            "agent.broadband.address_validated",
            self.handle_address_validated
        )
        
        self.logger.info(
            "Subscribed to peer communication channel: agent.broadband.address_validated"
        )

    async def handle_address_validated(self, event) -> None:
        """Handle address validation event from Utilities Agent.
        
        This demonstrates direct peer-to-peer communication:
        - Utilities Agent validates address
        - Publishes directly to this channel
        - Broadband Agent receives without Main Agent routing
        
        Args:
            event: AddressValidated event from Utilities Agent
        """
        from core.schemas import AddressValidated
        
        # RedisBus deserializes as BaseEvent — re-parse to correct type
        if not isinstance(event, AddressValidated):
            try:
                event = AddressValidated(**event.model_dump())
            except Exception as e:
                self.logger.warning(
                    f"Could not parse AddressValidated event: {e}",
                    event_type=type(event).__name__,
                )
                return
        
        self.logger.info(
            f"📥 Received address validation from Utilities Agent (direct peer communication)",
            correlation_id=event.correlation_id,
            address=event.address,
            electricity_available=event.electricity_available,
            gas_available=event.gas_available,
            service_area=event.service_area,
        )
        
        # Store validation result for use in broadband availability checks
        self.validated_addresses[event.address] = {
            "electricity_available": event.electricity_available,
            "gas_available": event.gas_available,
            "service_area": event.service_area,
            "estimated_connection_days": event.estimated_connection_days,
            "validation_timestamp": event.validation_timestamp,
            "validated_by": event.source_agent,
        }
        
        self.logger.debug(
            f"💾 Stored validation result for address: {event.address}",
            total_validated_addresses=len(self.validated_addresses),
        )

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

        # Check if we have validation data from Utilities Agent (peer communication)
        utilities_validation = self.validated_addresses.get(address)
        
        if utilities_validation:
            self.logger.info(
                f"🔍 Using address validation from Utilities Agent (received via peer communication)",
                address=address,
                service_area=utilities_validation.get("service_area"),
                validated_by=utilities_validation.get("validated_by"),
            )

        # Simulate availability check logic
        # In production, this would call ISP coverage APIs
        await asyncio.sleep(0.9)  # Simulate API call (realistic delay for demo)

        result = {
            "address": address,
            "fiber_available": True,
            "cable_available": True,
            "dsl_available": False,
            "max_speed_mbps": 1000,
            "providers": ["FastNet", "CableLink", "FiberPro"],
            "installation_required": True,
        }
        
        # Enhance result with utilities validation data if available
        if utilities_validation:
            result["utilities_validated"] = True
            result["service_area"] = utilities_validation.get("service_area")
            result["electricity_available"] = utilities_validation.get("electricity_available")
            result["gas_available"] = utilities_validation.get("gas_available")
            result["estimated_utilities_connection_days"] = utilities_validation.get("estimated_connection_days")
        else:
            result["utilities_validated"] = False
        
        return result

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
        await asyncio.sleep(1.1)  # Simulate API call (realistic delay for demo)

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


if __name__ == "__main__":
    """Run Broadband Agent as standalone service."""
    import asyncio

    async def main():
        """Initialize and start Broadband Agent."""
        config = Config()
        agent = BroadbandAgent(config)
        await agent.start()

        # Keep running until interrupted
        try:
            await asyncio.Event().wait()  # Blocks forever
        except (KeyboardInterrupt, asyncio.CancelledError):
            await agent.shutdown()

    asyncio.run(main())
