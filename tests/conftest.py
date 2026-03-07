import pytest
import pytest_asyncio
import redis.asyncio as redis

pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(autouse=True)
async def flush_redis():
    client = redis.from_url("redis://localhost:6379")
    await client.flushdb()
    yield
    await client.flushdb()
    await client.aclose()
