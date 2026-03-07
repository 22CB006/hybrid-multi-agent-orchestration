import pytest
import pytest_asyncio
import redis.asyncio as redis

from core.config import Config

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
def test_config():
    """Provide test configuration with dummy values."""
    return Config(
        redis_host="localhost",
        redis_port=6379,
        redis_password=None,
        redis_db=0,
        gemini_api_key="test_api_key_not_used",
        gemini_model="gemini-1.5-flash",
        gemini_timeout=2,
        retry_max_attempts=3,
        retry_base_delay=1.0,
        health_check_interval=30,
        health_check_timeout=10,
        log_level="INFO",
        task_timeout=30,
        dlq_retention_days=7,
        idempotency_ttl=3600,
    )


@pytest.fixture(autouse=True)
async def flush_redis():
    client = redis.from_url("redis://localhost:6379")
    await client.flushdb()
    yield
    await client.flushdb()
    await client.aclose()
