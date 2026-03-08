# Project Commands

## Prerequisites

- Redis must be running at `D:\Redis\`
- Python 3.12+ with uv package manager
- Virtual environment activated

## Setup

```powershell
# Install dependencies
uv sync

# Create environment file
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY
```

## Start the Application (Step-by-Step)

### Step 1 - Verify Redis
```powershell
& "D:\Redis\redis-cli.exe" ping
# Should return: PONG
```

### Step 2 - Start Redis Server (if not running)
```powershell
& "D:\Redis\redis-server.exe"
```
Keep this terminal open.

### Step 3 - Start Utilities Agent (Terminal 2)
```powershell
uv run python -m agents.utilities_agent
```
Keep this terminal open.

### Step 4 - Start Broadband Agent (Terminal 3)
```powershell
uv run python -m agents.broadband_agent
```
Keep this terminal open.

### Step 5 - Start API Server (Terminal 4)
```powershell
uv run python -m uvicorn api.main:app --reload
```

API available at: `http://localhost:8000`

## Quick Reference (All Commands)

```powershell
# Terminal 1 - Redis
& "D:\Redis\redis-server.exe"

# Terminal 2 - Utilities Agent
uv run python -m agents.utilities_agent

# Terminal 3 - Broadband Agent
uv run python -m agents.broadband_agent

# Terminal 4 - API Server
uv run python -m uvicorn api.main:app --reload
```

## Testing with PowerShell

### Submit a Task
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/tasks" -Method POST -ContentType "application/json" -Body '{"user_input": "I need electricity, gas, and internet at 123 Main St"}'
```

### Check Task Status (replace correlation_id)
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/tasks/YOUR_CORRELATION_ID" -Method GET
```

### Health Check
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/health" -Method GET
```

### Get Agent Registry
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/agents" -Method GET
```

## Run Tests

### Run All Tests

```powershell
uv run pytest -v -s
```

### Run Specific Test

```powershell
uv run pytest tests/integration/test_direct_agent_communication.py -v -s
```

### Run Unit Tests Only

```powershell
uv run pytest tests/unit/ -v
```

### Run Policy Enforcer Tests

```powershell
uv run pytest tests/unit/test_policy_enforcer.py -v
```

### Run with Coverage

```powershell
uv run pytest --cov=agents --cov=bus --cov=core --cov-report=term-missing
```

## Start the Application

See "Start the Application (Step-by-Step)" section above for detailed instructions.

Alternative using run.py:
```powershell
uv run python run.py
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

```powershell
# Compare centralized vs hybrid architecture performance
uv run python benchmark/benchmark.py
```

## Quick Demo

1. **Start Redis**:
   ```powershell
   & "D:\Redis\redis-server.exe"
   ```

2. **Run verification tests** (shows direct agent communication):
   ```powershell
   uv run pytest tests/integration/test_direct_agent_communication.py -v -s
   ```

3. **Expected output**:
   - ✓ 3 tests passed
   - ✓ Parallel execution verified (~0.2s)
   - ✓ No Main Agent routing confirmed
   - ✓ Direct pub/sub working

## Troubleshooting

### Redis Connection Error

```powershell
# Check if Redis is running
& "D:\Redis\redis-cli.exe" ping

# If not running, start it
& "D:\Redis\redis-server.exe"
```

### Import Errors

```powershell
# Reinstall dependencies
uv sync
```

### Test Failures

```powershell
# Flush Redis database
& "D:\Redis\redis-cli.exe" FLUSHDB

# Rerun tests
uv run pytest tests/integration/test_direct_agent_communication.py -v -s
```
