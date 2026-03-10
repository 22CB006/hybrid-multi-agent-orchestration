# Hybrid Multi-Agent Orchestration

A working prototype that solves the centralised multi-agent bottleneck through a **Hybrid Control/Data Plane architecture** with three live comparison modes.

---

## The Problem

In a centralised multi-agent system, every message routes through the Main Agent:

```
User → Main Agent → Utilities Agent → Main Agent → Broadband Agent → Main Agent → User
```

**Consequences:**
- Main Agent CPU at ~100% routing messages instead of orchestrating
- Tasks execute sequentially — no parallelism
- Response time grows linearly with agent count: O(N)
- Single point of failure for the entire workflow

---

## The Solution

Separate the **Control Plane** (orchestration, monitoring, policy) from the **Data Plane** (task execution, agent communication):

```
┌──────────────────────────────────────────────┐
│         MAIN AGENT (Control Plane)           │
│  Monitoring · Policy · Logging · Routing     │
└─────────────────────┬────────────────────────┘
                      │ observes via agent.*.*
┌─────────────────────▼────────────────────────┐
│         REDIS MESSAGE BUS (Data Plane)       │
└─────────┬────────────────────────┬───────────┘
          │                        │
     ┌────▼──────┐           ┌─────▼─────┐
     │ Utilities │◄─────────►│ Broadband │
     │   Agent   │ peer comm │   Agent   │
     └───────────┘           └───────────┘
```

The Main Agent publishes tasks **once** to the bus. Agents consume independently and in parallel. The Main Agent **observes** everything but sits in the critical path for nothing.

---

## Three-Mode Live Comparison

The Streamlit demo runs all three modes against the same request and displays results side by side.

| Mode | Architecture | Routing | Execution | Hops | Main Agent Load |
|------|-------------|---------|-----------|------|-----------------|
| Mode 1 | Centralised | Static | Sequential | 12 | ~100% |
| Mode 2 | Hybrid (this project) | Keywords | Parallel via Redis | 7 | ~15% |
| Mode 3 | Hybrid + LLM | DeepSeek v3 via OpenRouter | Parallel + intelligent routing | 9 | ~25% |

**Representative demo results** (same request, same machine, same simulated delays):

| Metric | Mode 1 | Mode 2 | Mode 3 |
|--------|--------|--------|--------|
| Response time | 5.14s | 3.84s | 9.61s |
| vs Mode 1 | baseline | **−1.31s (25% faster)** | +4.47s (LLM overhead) |

> Mode 3 is slower due to ~8s OpenRouter API latency — a deliberate demonstration that intelligence has a cost. The mitigation (async caching + parallel fallback) is documented in the architecture review.

---

## Scalability

Centralised routing grows O(N) with agent count. Hybrid stays nearly O(1):

| Agents | Centralised Hops | Hybrid Hops | Centralised Time | Hybrid Time |
|--------|-----------------|-------------|-----------------|-------------|
| 2 | 4 (sequential) | 4 (parallel) | ~2.2s | ~1.1s |
| 5 | 10 (sequential) | 7 (parallel) | ~5.0s | ~1.3s |
| 10 | 20 (sequential) | 12 (parallel) | ~10.0s | ~1.5s |
| 20 | 40 (sequential) | 22 (parallel) | ~20.0s | ~1.8s |

---

## Quick Start

**Prerequisites:** Python 3.12+, Redis 7.2+, uv

```bash
# 1. Clone
git clone https://github.com/22CB006/hybrid-multi-agent-orchestration.git
cd hybrid-multi-agent-orchestration

# 2. Install dependencies
uv sync

# 3. Configure environment
cp .env.example .env
# Add OPENROUTER_API_KEY to .env for Mode 3

# 4. Start Redis
redis-server

# 5. Start API server
uv run python run.py

# 6. Launch demo (separate terminal)
streamlit run demo/app.py
```

Open **http://localhost:8501** to run the three-mode comparison.

---

## Project Structure

```
hybrid-multi-agent-orchestration/
├── agents/
│   ├── base_agent.py              # Idempotency, health checks, failure handling
│   ├── main_agent.py              # Control Plane — orchestration & monitoring
│   ├── utilities_agent.py         # Data Plane — electricity/gas setup
│   ├── broadband_agent.py         # Data Plane — internet setup
│   └── sequential_orchestrator.py # Mode 1 baseline (no Redis, no parallelism)
├── bus/
│   └── redis_bus.py               # Redis Pub/Sub — reconnection, wildcard subscribe
├── core/
│   ├── agent_registry.py          # Heartbeat tracking, health monitoring
│   ├── dead_letter_queue.py       # Failed event storage, 7-day retention, replay
│   ├── openrouter_parser.py       # DeepSeek v3 via OpenRouter — Mode 3 routing
│   ├── policy_enforcer.py         # Channel isolation + rate limiting
│   ├── logger.py                  # Structured logging with correlation_id threading
│   ├── schemas.py                 # Pydantic event models
│   └── config.py                  # Configuration management
├── api/
│   └── main.py                    # FastAPI — /compare and management endpoints
├── demo/
│   └── app.py                     # Streamlit three-mode comparison UI
├── tests/                         # Test suite (planned — not yet implemented)
├── pyproject.toml
├── pytest.ini
└── run.py
```

---

## API Reference

### Core
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/compare` | Run all three modes against the same request |
| `POST` | `/tasks` | Submit a task request |
| `GET` | `/tasks/{correlation_id}` | Retrieve task status and results |

### System
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | System health status |
| `GET` | `/agents` | Active agent registry |
| `GET` | `/dead-letter-queue` | View failed events |
| `POST` | `/dead-letter-queue/{event_id}/retry` | Replay a failed event |

---

## Event Schema

All messages conform to a shared Pydantic schema:

```json
{
  "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-01-15T10:01:22Z",
  "event_type": "task_request",
  "source_agent": "main",
  "target_agent": "utilities",
  "task_type": "setup_electricity",
  "payload": {
    "address": "123 Main St",
    "move_date": "2025-02-15"
  }
}
```

---

## Reliability Features

| Scenario | Strategy |
|----------|----------|
| Task fails | Auto-retry up to 3 times with exponential backoff (`delay = base × 2^n`) |
| All retries exhausted | Moved to Dead Letter Queue with 7-day retention |
| Duplicate event delivered | Idempotency cache (`correlation_id:task_type` key) prevents re-execution |
| Agent stops heartbeating | Flagged unhealthy after 3 missed checks; excluded from routing |
| Redis reconnects | Exponential backoff reconnection (5 attempts: 1s → 16s) |

---

## Policy Enforcement

Two policies are enforced on every publish operation by `core/policy_enforcer.py`:

### Policy 1 — Channel Isolation

Agents may only publish to their own channels or an explicit peer whitelist. The Main Agent is exempt as the system orchestrator.

```python
allowed_peer_channels = {
    "utilities": ["agent.broadband.address_validated"],
    "broadband": ["agent.utilities.address_validated"],
}
```

| Publish attempt | Allowed? |
|----------------|----------|
| `utilities → agent.utilities.task_response` | ✅ own channel |
| `utilities → agent.broadband.address_validated` | ✅ whitelisted peer |
| `utilities → agent.broadband.task_request` | ❌ blocked — input channel |
| `main → agent.utilities.task_request` | ✅ orchestrator privilege |

Violations are dropped, logged, and stored in Redis for a 7-day audit trail.

### Policy 2 — Rate Limiting

Maximum **100 events per `correlation_id` per 60 seconds**, tracked via a Redis counter with TTL.

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_HOST` | Redis hostname | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `OPENROUTER_API_KEY` | Required for Mode 3 LLM routing | — |
| `LOG_LEVEL` | Logging verbosity | `INFO` |
| `ENVIRONMENT` | Runtime environment | `development` |

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| API | FastAPI |
| Message Bus | Redis Pub/Sub |
| Schema Validation | Pydantic v2 |
| LLM Routing (Mode 3) | DeepSeek v3 via OpenRouter |
| Demo UI | Streamlit |
| Package Manager | uv |

## Implementation Notes

This hybrid architecture separates orchestration concerns from execution flow:

- **Control Plane**: Main Agent monitors communications via pattern subscriptions (`agent.*.*`) and enforces policies without blocking data flow
- **Data Plane**: Redis Pub/Sub enables direct agent communication and parallel task execution
- **Efficiency**: Eliminates sequential routing bottlenecks present in centralized architectures

The implementation demonstrates that control and observability can be maintained while achieving better performance through architectural separation.

## Author

Arya Lakshmi M

GitHub: [@22CB006](https://github.com/22CB006)
