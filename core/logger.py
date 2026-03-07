"""Structured logging with correlation tracking for multi-agent system."""

import json
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Optional, TextIO
from uuid import UUID


class StructuredLogger:
    """
    Provides JSON-formatted logging with correlation tracking.

    Outputs structured logs to stdout for container deployment with
    standard fields and optional correlation tracking for distributed tracing.
    """

    def __init__(
        self,
        agent_name: str,
        log_level: str = "INFO",
        output_stream: TextIO = sys.stdout,
    ):
        """
        Initialize structured logger.

        Args:
            agent_name: Name of agent using this logger
            log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            output_stream: Output destination (default: stdout)
        """
        self.agent_name = agent_name
        self.log_level = log_level.upper()
        self.output_stream = output_stream

        # Define log level hierarchy
        self._log_levels = {
            "DEBUG": 10,
            "INFO": 20,
            "WARNING": 30,
            "ERROR": 40,
            "CRITICAL": 50,
        }

    def _should_log(self, level: str) -> bool:
        """Check if message should be logged based on configured level."""
        return self._log_levels.get(level.upper(), 0) >= self._log_levels.get(
            self.log_level, 0
        )

    def log(
        self,
        level: str,
        message: str,
        correlation_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        """
        Write structured log entry in JSON format.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Human-readable log message
            correlation_id: Optional UUID for request tracing
            **kwargs: Additional context fields to include in log entry
        """
        level = level.upper()

        # Skip if below configured log level
        if not self._should_log(level):
            return

        # Build log entry with standard fields
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "agent_name": self.agent_name,
            "message": message,
        }

        # Add correlation_id if provided
        if correlation_id is not None:
            log_entry["correlation_id"] = str(correlation_id)

        # Add stack trace for ERROR level logs
        if level in ("ERROR", "CRITICAL"):
            stack_trace = traceback.format_exc()
            # Only include if there's an actual exception
            if stack_trace and stack_trace != "NoneType: None\n":
                log_entry["stack_trace"] = stack_trace

        # Merge additional context
        log_entry.update(kwargs)

        # Serialize to JSON and write to output stream
        try:
            json_output = json.dumps(log_entry, default=str)
            self.output_stream.write(json_output + "\n")
            self.output_stream.flush()
        except Exception as e:
            # Fallback to plain text if JSON serialization fails
            fallback = f"[{level}] {self.agent_name}: {message} (JSON serialization failed: {e})\n"
            self.output_stream.write(fallback)
            self.output_stream.flush()

    def debug(
        self, message: str, correlation_id: Optional[UUID] = None, **kwargs: Any
    ) -> None:
        """Log DEBUG level message."""
        self.log("DEBUG", message, correlation_id, **kwargs)

    def info(
        self, message: str, correlation_id: Optional[UUID] = None, **kwargs: Any
    ) -> None:
        """Log INFO level message."""
        self.log("INFO", message, correlation_id, **kwargs)

    def warning(
        self, message: str, correlation_id: Optional[UUID] = None, **kwargs: Any
    ) -> None:
        """Log WARNING level message."""
        self.log("WARNING", message, correlation_id, **kwargs)

    def error(
        self, message: str, correlation_id: Optional[UUID] = None, **kwargs: Any
    ) -> None:
        """Log ERROR level message."""
        self.log("ERROR", message, correlation_id, **kwargs)

    def critical(
        self, message: str, correlation_id: Optional[UUID] = None, **kwargs: Any
    ) -> None:
        """Log CRITICAL level message."""
        self.log("CRITICAL", message, correlation_id, **kwargs)
