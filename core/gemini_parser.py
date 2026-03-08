"""Gemini-based natural language input parser for multi-agent orchestration.

Uses Gemini 1.5 Flash to convert user requests into structured intents
that can be routed to appropriate agents.
"""

import asyncio
import json
from typing import List
from uuid import UUID

import google.generativeai as genai
from pydantic import BaseModel, Field, ValidationError

from core.logger import StructuredLogger


class ParsedIntent(BaseModel):
    """Structured output from LLM parser."""

    intent: str = Field(..., description="Primary user intent (e.g., 'setup_services')")
    entities: dict = Field(
        ..., description="Extracted entities (address, move_date, etc.)"
    )
    target_agents: List[str] = Field(
        ..., description="Agents that should handle request"
    )
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Parser confidence score"
    )


class GeminiParser:
    """
    Converts natural language user input to structured JSON using Gemini 1.5 Flash.

    Extracts intent, entities, and target agents from user requests to enable
    intelligent routing in the multi-agent system.
    """

    def __init__(
        self,
        api_key: str,
        model: str = "gemini-1.5-flash",
        timeout_seconds: int = 2,
        logger: StructuredLogger = None,
    ):
        """
        Initialize Gemini Parser.

        Args:
            api_key: Gemini API authentication key
            model: Gemini model name (default: gemini-1.5-flash)
            timeout_seconds: Request timeout (default: 2)
            logger: Optional structured logger instance
        """
        self.api_key = api_key
        self.model_name = model
        self.timeout_seconds = timeout_seconds
        self.logger = logger or StructuredLogger("gemini_parser")

        # Configure Gemini API
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(self.model_name)

    def _construct_prompt(self, user_input: str) -> str:
        """
        Construct prompt template for intent extraction.

        Args:
            user_input: Raw user request text

        Returns:
            Formatted prompt for Gemini API
        """
        return f"""You are an AI assistant helping users move to a new home. Parse the following user request and extract:
1. The primary intent (what the user wants to accomplish)
2. Key entities such as:
   - address: The physical address mentioned
   - move_date: When they're moving (if mentioned)
   - services: List of services they need (electricity, gas, internet, broadband, etc.)
   - service_preferences: Any specific preferences or requirements
3. Which agents should handle this request:
   - "utilities" for electricity and gas setup
   - "broadband" for internet and broadband setup
   - Include both if the user needs multiple services

User input: "{user_input}"

Respond with ONLY valid JSON in this exact format (no markdown, no code blocks, just raw JSON):
{{
  "intent": "setup_services",
  "entities": {{
    "address": "123 Main St",
    "move_date": "2024-02-15",
    "services": ["electricity", "gas", "internet"]
  }},
  "target_agents": ["utilities", "broadband"],
  "confidence": 0.95
}}

Rules:
- If address is not mentioned, omit the "address" field
- If move_date is not mentioned, omit the "move_date" field
- Always include "services" as a list (can be empty if unclear)
- target_agents must be a list containing "utilities", "broadband", or both
- confidence should be between 0.0 and 1.0
- Return ONLY the JSON object, nothing else"""

    async def parse_input(self, user_input: str, correlation_id: UUID) -> ParsedIntent:
        """
        Parse natural language to structured intent.

        Args:
            user_input: Raw user request text
            correlation_id: Request tracking ID for logging

        Returns:
            ParsedIntent object with extracted information

        Raises:
            TimeoutError: If parsing exceeds timeout_seconds
            ValidationError: If response doesn't match schema
            ValueError: If parsing fails for other reasons
        """
        self.logger.info(
            f"Parsing user input: {user_input[:100]}...",
            correlation_id=correlation_id,
            input_length=len(user_input),
        )

        try:
            # Construct prompt
            prompt = self._construct_prompt(user_input)

            # Call Gemini API with timeout
            response = await asyncio.wait_for(
                self._call_gemini_api(prompt, correlation_id),
                timeout=self.timeout_seconds,
            )

            # Parse and validate response
            parsed_intent = self._validate_response(response, correlation_id)

            self.logger.info(
                "Successfully parsed user input",
                correlation_id=correlation_id,
                intent=parsed_intent.intent,
                target_agents=parsed_intent.target_agents,
                confidence=parsed_intent.confidence,
            )

            return parsed_intent

        except asyncio.TimeoutError:
            self.logger.error(
                f"Gemini API call timed out after {self.timeout_seconds}s",
                correlation_id=correlation_id,
            )
            raise TimeoutError(
                f"Parsing timed out after {self.timeout_seconds} seconds"
            )

        except ValidationError as e:
            self.logger.error(
                "Response validation failed",
                correlation_id=correlation_id,
                validation_errors=str(e),
            )
            raise ValidationError(f"Invalid response schema from Gemini: {e}") from e

        except Exception as e:
            self.logger.error(
                "Parsing failed with unexpected error",
                correlation_id=correlation_id,
                error_type=type(e).__name__,
                error_message=str(e),
            )
            raise ValueError(f"Failed to parse input: {str(e)}") from e

    async def _call_gemini_api(self, prompt: str, correlation_id: UUID) -> str:
        """
        Call Gemini API asynchronously.

        Args:
            prompt: Formatted prompt for the model
            correlation_id: Request tracking ID

        Returns:
            Raw response text from Gemini

        Raises:
            Exception: If API call fails
        """
        try:
            # Run synchronous Gemini call in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, lambda: self.model.generate_content(prompt)
            )

            if not response or not response.text:
                raise ValueError("Empty response from Gemini API")

            return response.text.strip()

        except Exception as e:
            self.logger.error(
                "Gemini API call failed",
                correlation_id=correlation_id,
                error_type=type(e).__name__,
                error_message=str(e),
            )
            raise

    def _validate_response(
        self, response_text: str, correlation_id: UUID
    ) -> ParsedIntent:
        """
        Validate response schema and parse to ParsedIntent.

        Args:
            response_text: Raw JSON response from Gemini
            correlation_id: Request tracking ID

        Returns:
            Validated ParsedIntent object

        Raises:
            ValidationError: If response doesn't match schema
            ValueError: If JSON parsing fails
        """
        try:
            # Clean response text (remove markdown code blocks if present)
            cleaned_text = response_text.strip()
            if cleaned_text.startswith("```"):
                # Remove markdown code block markers
                lines = cleaned_text.split("\n")
                # Remove first line (```json or ```)
                lines = lines[1:]
                # Remove last line (```)
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                cleaned_text = "\n".join(lines).strip()

            # Parse JSON
            response_dict = json.loads(cleaned_text)

            # Validate against Pydantic model
            parsed_intent = ParsedIntent(**response_dict)

            return parsed_intent

        except json.JSONDecodeError as e:
            self.logger.error(
                "Failed to parse JSON response",
                correlation_id=correlation_id,
                response_text=response_text[:200],
                error=str(e),
            )
            raise ValueError(f"Invalid JSON in response: {e}") from e

        except ValidationError as e:
            self.logger.error(
                "Response validation failed",
                correlation_id=correlation_id,
                response_text=response_text[:200],
                validation_errors=str(e),
            )
            raise
