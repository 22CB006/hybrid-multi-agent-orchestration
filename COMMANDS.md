# Project Commands

## Prerequisites

- Redis must be running
- Start Redis: `redis-server` or check Windows service
- Verify Redis: `redis-cli ping` (should return PONG)

## Setup

```bash
# Install dependencies
uv sync

# Create environment file
cp .env.example .env
```

## Run Tests

### Day 2 - Direct Agent Communication (Core Architecture Verification)

```bash
python -m pytest tests/integration/test_direct_agent_communication.py -v -s
```

**What this proves:**
- ✓ Utilities → Redis → Broadband (no Main Agent routing)
- ✓ Parallel execution (0.2s, not sequential)
- ✓ Data plane works independently of control plane

### Run All Tests

```bash
python -m pytest -v -s
```

### Run Specific Test

```bash
python -m pytest tests/integration/test_direct_agent_communication.py::test_parallel_agent_execution -v -s
```

### Run Unit Tests Only

```bash
python -m pytest tests/unit/ -v
```

### Run Policy Enforcer Tests

```bash
uv run python -m pytest tests/unit/test_policy_enforcer.py -v
```

### Run with Coverage

```bash
python -m pytest --cov=agents --cov=bus --cov=core --cov-report=term-missing
```

## Start the Application

```bash
# Start API server
python run.py

# Or with uvicorn directly
uv run uvicorn api.main:app --reload
```

## Development

### Check Code Quality

```bash
# Run linter
ruff check .

# Format code
ruff format .
```

### Run Benchmark

```bash
# Compare centralized vs hybrid architecture performance
python benchmark/benchmark.py
```

## Quick Demo for Interview

1. **Start Redis** (in separate terminal):
   ```bash
   redis-server
   ```

2. **Run verification tests** (shows direct agent communication):
   ```bash
   python -m pytest tests/integration/test_direct_agent_communication.py -v -s
   ```

3. **Expected output**:
   - ✓ 3 tests passed
   - ✓ Parallel execution verified (~0.2s)
   - ✓ No Main Agent routing confirmed
   - ✓ Direct pub/sub working

## Troubleshooting

### Redis Connection Error

```bash
# Check if Redis is running
redis-cli ping

# If not running, start it
redis-server
```

### Import Errors

```bash
# Reinstall dependencies
uv sync
```

### Test Failures

```bash
# Flush Redis database
redis-cli FLUSHDB

# Rerun tests
python -m pytest tests/integration/test_direct_agent_communication.py -v -s
```
