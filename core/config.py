"""Configuration management using pydantic-settings."""

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Config(BaseSettings):
    """Application configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # Redis Configuration
    redis_host: str = Field(default="localhost", description="Redis server host")
    redis_port: int = Field(default=6379, description="Redis server port")
    redis_password: Optional[str] = Field(
        default=None, description="Redis authentication password"
    )
    redis_db: int = Field(default=0, description="Redis database number")

    # Gemini Configuration
    gemini_api_key: str = Field(..., description="Gemini API key for LLM parsing")
    gemini_model: str = Field(
        default="gemini-2.0-flash", description="Gemini model name"
    )
    gemini_timeout: int = Field(default=2, description="Gemini API timeout in seconds")

    # Retry Configuration
    retry_max_attempts: int = Field(
        default=3, description="Maximum retry attempts for failed tasks"
    )
    retry_base_delay: float = Field(
        default=1.0, description="Base delay for exponential backoff in seconds"
    )

    # Health Check Configuration
    health_check_interval: int = Field(
        default=30, description="Health check interval in seconds"
    )
    health_check_timeout: int = Field(
        default=10, description="Health check timeout in seconds"
    )

    # Logging Configuration
    log_level: str = Field(default="INFO", description="Logging level")

    # Task Configuration
    task_timeout: int = Field(default=30, description="Default task timeout in seconds")

    # Dead Letter Queue Configuration
    dlq_retention_days: int = Field(
        default=7, description="DLQ event retention period in days"
    )

    # Idempotency Configuration
    idempotency_ttl: int = Field(
        default=3600, description="Idempotency cache TTL in seconds"
    )

    @property
    def redis_url(self) -> str:
        """Construct Redis connection URL."""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @field_validator("redis_port")
    @classmethod
    def validate_redis_port(cls, v: int) -> int:
        """Validate Redis port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError("Redis port must be between 1 and 65535")
        return v

    @field_validator("redis_db")
    @classmethod
    def validate_redis_db(cls, v: int) -> int:
        """Validate Redis database number."""
        if not 0 <= v <= 15:
            raise ValueError("Redis database must be between 0 and 15")
        return v

    @field_validator("gemini_timeout")
    @classmethod
    def validate_gemini_timeout(cls, v: int) -> int:
        """Validate Gemini timeout is positive."""
        if v <= 0:
            raise ValueError("Gemini timeout must be positive")
        return v

    @field_validator("retry_max_attempts")
    @classmethod
    def validate_retry_max_attempts(cls, v: int) -> int:
        """Validate retry max attempts is positive."""
        if v <= 0:
            raise ValueError("Retry max attempts must be positive")
        return v

    @field_validator("retry_base_delay")
    @classmethod
    def validate_retry_base_delay(cls, v: float) -> float:
        """Validate retry base delay is positive."""
        if v <= 0:
            raise ValueError("Retry base delay must be positive")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v_upper

    def validate_config(self) -> None:
        """
        Validate configuration after loading.

        Raises:
            ValueError: If configuration is invalid
        """
        # Check required fields
        if not self.gemini_api_key:
            raise ValueError("GEMINI_API_KEY is required but not set")

        # Validate Redis connection parameters
        if not self.redis_host:
            raise ValueError("REDIS_HOST cannot be empty")

        # Validate timeout values are reasonable
        if self.task_timeout <= 0:
            raise ValueError("Task timeout must be positive")

        if self.health_check_interval <= 0:
            raise ValueError("Health check interval must be positive")

        if self.health_check_timeout <= 0:
            raise ValueError("Health check timeout must be positive")

        if self.health_check_timeout >= self.health_check_interval:
            raise ValueError("Health check timeout must be less than interval")

        # Validate DLQ retention
        if self.dlq_retention_days <= 0:
            raise ValueError("DLQ retention days must be positive")

        # Validate idempotency TTL
        if self.idempotency_ttl <= 0:
            raise ValueError("Idempotency TTL must be positive")
