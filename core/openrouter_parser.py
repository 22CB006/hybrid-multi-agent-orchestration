"""OpenRouter-based intent parser for multi-agent orchestration.

Uses DeepSeek v3.2 via OpenRouter API to classify user input and route
to the correct agents. This is Mode 3 routing — LLM-powered, no keyword maps.

API: https://openrouter.ai/api/v1/chat/completions
Model: deepseek/deepseek-v3.2
"""

import json
import time
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

import httpx

from core.logger import StructuredLogger


@dataclass
class ParsedIntent:
    """Structured representation of parsed user intent."""
    intent: str
    entities: Dict[str, Any]
    target_agents: List[str]
    confidence: float


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "deepseek/deepseek-v3.2"

SYSTEM_PROMPT = """You are a routing agent for a home-moving assistant service.
Analyse the user request and determine which specialist agents to activate.

Available agents:
- utilities: handles electricity connection, gas setup, meter installation, energy tariffs
- broadband: handles internet setup, WiFi, fibre broadband, ISP plans, router installation

Return ONLY a JSON object with exactly these fields:
{
  "intent": "one of: setup_services | validate_address | get_quote | check_availability | general_inquiry",
  "entities": {"address": "extracted address or null", "services": ["list of services mentioned"]},
  "target_agents": ["list of agents from: utilities, broadband"],
  "confidence": 0.0 to 1.0
}

Rules:
- If the request mentions electricity, gas, power, meter, energy → include "utilities"
- If the request mentions internet, broadband, wifi, fibre, router, ISP → include "broadband"
- If general home setup / moving request → include both agents
- Always include at least one agent"""


class OpenRouterParser:
    """
    Routes user requests to agents using DeepSeek v3.2 via OpenRouter.

    Mode 3 alternative to keyword matching and Gemini — demonstrates
    how any LLM can be swapped in via a standard OpenAI-compatible API.
    """

    def __init__(
        self,
        api_key: str,
        timeout_seconds: int = 8,
        logger: Optional[StructuredLogger] = None,
    ):
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.logger = logger or StructuredLogger("openrouter_parser")
        self._total_calls = 0

        print(f"\n{'='*70}")
        print(f"  OPENROUTER PARSER INITIALIZED")
        print(f"{'='*70}")
        print(f"   Model   : {OPENROUTER_MODEL}")
        print(f"   API Key : {self.api_key[:12]}...{self.api_key[-8:]}")
        print(f"   Timeout : {self.timeout_seconds}s")
        print(f"{'='*70}\n")

    async def parse_input(self, user_input: str) -> ParsedIntent:
        """
        Call OpenRouter API to classify input and determine target agents.

        Args:
            user_input: Raw natural language from user

        Returns:
            ParsedIntent with routing decision from DeepSeek

        Raises:
            Exception: If API call fails (caller should fall back to keywords)
        """
        self._total_calls += 1
        call_num = self._total_calls

        print(f"\n{'─'*70}")
        print(f"  OPENROUTER API CALL #{call_num}")
        print(f"  Input : {user_input!r}")
        print(f"{'─'*70}")

        self.logger.info(
            "Calling OpenRouter API for intent routing",
            call_number=call_num,
            user_input=user_input,
            model=OPENROUTER_MODEL,
        )

        t_start = time.perf_counter()

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(
                OPENROUTER_URL,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}",
                },
                json={
                    "model": OPENROUTER_MODEL,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_input},
                    ],
                    "response_format": {"type": "json_object"},
                },
            )
            response.raise_for_status()

        elapsed_ms = (time.perf_counter() - t_start) * 1000
        data = response.json()
        raw_content = data["choices"][0]["message"]["content"]

        print(f"  Response ({elapsed_ms:.0f}ms): {raw_content}")
        print(f"{'─'*70}\n")

        parsed = json.loads(raw_content)

        # Ensure target_agents is always populated
        target_agents = parsed.get("target_agents", [])
        if not target_agents:
            target_agents = ["utilities", "broadband"]

        intent = ParsedIntent(
            intent=parsed.get("intent", "setup_services"),
            entities=parsed.get("entities", {"address": None, "services": []}),
            target_agents=target_agents,
            confidence=float(parsed.get("confidence", 0.9)),
        )

        self.logger.info(
            "OpenRouter routing decision",
            intent=intent.intent,
            target_agents=intent.target_agents,
            confidence=intent.confidence,
            latency_ms=round(elapsed_ms),
        )

        return intent, round(elapsed_ms)
