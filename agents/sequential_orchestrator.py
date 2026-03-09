"""Sequential Orchestrator — Mode 1: No Message Bus.

Demonstrates the PROBLEM this architecture solves.

All tasks are routed through Main Agent sequentially:
  Main Agent → Utilities Agent → Main Agent → Broadband Agent → Main Agent

Every hop is a blocking call. No parallelism. Total time = SUM of all tasks.

Uses the SAME task methods and delays as the real agents — only the
orchestration pattern changes (sequential vs parallel pub/sub).
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from agents.utilities_agent import UtilitiesAgent
from agents.broadband_agent import BroadbandAgent
from core.config import Config
from core.logger import StructuredLogger


@dataclass
class HopRecord:
    number: int
    source: str
    target: str
    message: str
    hop_type: str          # "request" | "response" | "internal"
    duration_ms: float = 0.0


class SequentialOrchestrator:
    """
    Mode 1: Direct sequential orchestration with no message bus.

    Instantiates the real agent classes and calls their task methods
    directly — NO Redis, NO pub/sub, NO parallelism.

    This is the "naive" architecture that demonstrates why a message
    bus with parallel execution is needed.
    """

    def __init__(self, config: Config):
        self.config = config
        self.logger = StructuredLogger("sequential_orchestrator", config.log_level)

        # Create agents WITHOUT calling .start() — no Redis connection
        # We call their internal task methods directly instead
        self._utilities = UtilitiesAgent(config)
        self._broadband = BroadbandAgent(config)

        self.hops: List[HopRecord] = []
        self._hop_n = 0

    def _hop(
        self,
        source: str,
        target: str,
        message: str,
        hop_type: str = "request",
        duration_ms: float = 0.0,
    ) -> HopRecord:
        self._hop_n += 1
        record = HopRecord(self._hop_n, source, target, message, hop_type, duration_ms)
        self.hops.append(record)
        return record

    async def run(self, user_input: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute all tasks sequentially, tracking every hop.

        Args:
            user_input: Original user request string
            payload: Task payload (must include 'address')

        Returns:
            Dict with total_time, hop_count, hops list, and all task results
        """
        self.hops = []
        self._hop_n = 0
        results: Dict[str, Any] = {}

        wall_start = time.perf_counter()

        self.logger.info("Starting sequential orchestration (Mode 1 — No Bus)", user_input=user_input)

        # ── Hop 1: User → Main Agent ─────────────────────────────────────────
        self._hop("User", "Main Agent", f'"{user_input}"', "request")

        # ── Hop 2-3: validate_address ─────────────────────────────────────────
        self._hop("Main Agent", "Utilities Agent", "validate_address", "request")
        t = time.perf_counter()
        results["validate_address"] = await self._utilities._validate_address(payload)
        dur = (time.perf_counter() - t) * 1000
        self._hop("Utilities Agent", "Main Agent", "address_validated ✓", "response", dur)
        self.logger.info(f"Hop 2-3 done: validate_address ({dur:.0f}ms)")

        # ── Hop 4-5: setup_electricity ────────────────────────────────────────
        self._hop("Main Agent", "Utilities Agent", "setup_electricity", "request")
        t = time.perf_counter()
        results["setup_electricity"] = await self._utilities._setup_electricity(payload)
        dur = (time.perf_counter() - t) * 1000
        self._hop("Utilities Agent", "Main Agent", "electricity_ready ✓", "response", dur)
        self.logger.info(f"Hop 4-5 done: setup_electricity ({dur:.0f}ms)")

        # ── Hop 6-7: setup_gas ───────────────────────────────────────────────
        self._hop("Main Agent", "Utilities Agent", "setup_gas", "request")
        t = time.perf_counter()
        results["setup_gas"] = await self._utilities._setup_gas(payload)
        dur = (time.perf_counter() - t) * 1000
        self._hop("Utilities Agent", "Main Agent", "gas_ready ✓", "response", dur)
        self.logger.info(f"Hop 6-7 done: setup_gas ({dur:.0f}ms)")

        # ── Hop 8-9: check_availability ──────────────────────────────────────
        # Main Agent must wait for utilities to finish before calling broadband
        # (no peer communication without the bus)
        self._hop("Main Agent", "Broadband Agent", "check_availability", "request")
        t = time.perf_counter()
        results["check_availability"] = await self._broadband._check_availability(payload)
        dur = (time.perf_counter() - t) * 1000
        self._hop("Broadband Agent", "Main Agent", "availability_confirmed ✓", "response", dur)
        self.logger.info(f"Hop 8-9 done: check_availability ({dur:.0f}ms)")

        # ── Hop 10-11: setup_internet ─────────────────────────────────────────
        self._hop("Main Agent", "Broadband Agent", "setup_internet", "request")
        t = time.perf_counter()
        results["setup_internet"] = await self._broadband._setup_internet(payload)
        dur = (time.perf_counter() - t) * 1000
        self._hop("Broadband Agent", "Main Agent", "internet_ready ✓", "response", dur)
        self.logger.info(f"Hop 10-11 done: setup_internet ({dur:.0f}ms)")

        # ── Hop 12: Main Agent → User ─────────────────────────────────────────
        self._hop("Main Agent", "User", "all services configured ✓", "response")

        total_time = time.perf_counter() - wall_start

        self.logger.info(
            "Sequential orchestration complete",
            total_time_s=round(total_time, 2),
            hop_count=self._hop_n,
        )

        return {
            "mode": "Mode 1 — No Redis Bus (Sequential)",
            "routing": "Direct function calls",
            "parallelism": "None — fully sequential",
            "total_time": round(total_time, 3),
            "hop_count": self._hop_n,
            "hops": [
                {
                    "number": h.number,
                    "source": h.source,
                    "target": h.target,
                    "message": h.message,
                    "type": h.hop_type,
                    "duration_ms": round(h.duration_ms, 1),
                }
                for h in self.hops
            ],
            "results": results,
        }
