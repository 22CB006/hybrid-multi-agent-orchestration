# Hybrid Multi-Agent Orchestration System

Hybrid Control-Plane / Data-Plane Architecture for Multi-Agent Communication

## Overview

This project implements a hybrid architecture that separates governance (Control Plane) from execution (Data Plane), enabling direct agent-to-agent communication while maintaining full observability. The system is designed to eliminate routing bottlenecks in home moving assistance and solve scalability issues in multi-agent orchestration.

### Key Features

- Direct Agent Communication - Sub-agents communicate via Redis message bus without routing through Main Agent
- Parallel Execution - Multiple agents process tasks concurrently
- Full Observability - Main Agent monitors all system events in real-time
- Fault Tolerance - Automatic retry logic with dead-letter queue for failed tasks
- Scalable Design - Add new agents without modifying existing system components

### Performance Improvements

| Metric | Centralized | Hybrid | Improvement |
|--------|-------------|--------|-------------|
| Average Latency | ~2.4s | ~1.2s | 50% reduction |
| Throughput | ~15 workflows/sec | ~30 workflows/sec | 2x increase |
| Message Hops | 4 hops | 2 hops | 50% fewer |
| Main Agent Load | 100% | ~10% | 90% reduction |

## Architecture

```
┌─────────────────────────────────────────────────┐
│         MAIN AGENT                              │
│         CONTROL PLANE                           │
│   Monitoring | Policy | Logging | Escalation   │
└────────────────────┬────────────────────────────┘
                     │ observes (non-blocking)
                     │
┌────────────────────▼────────────────────────────┐
│         REDIS MESSAGE BUS                       │
│         (Data Plane)                            │
└────────┬───────────────────────────┬────────────┘
         │                           │
    ┌────▼─────┐              ┌─────▼────┐
    │ Utilities│◄────────────►│Broadband │
    │  Agent   │   direct     │  Agent   │
    └──────────┘   comms      └──────────┘
```

### Control Plane Responsibilities
- Task orchestration and delegation
- Policy enforcement and validation
- System-wide monitoring and logging
- Failure escalation and recovery
- Agent health checks

### Data Plane Responsibilities
- Direct agent-to-agent communication
- Parallel task execution
- Event publishing and subscription
- Result reporting

## Project Structure

```
hybrid-multi-agent-orchestration/
├── agents/                      # Agent implementations
│   ├── __init__.py
│   ├── main_agent.py           # Control Plane - orchestration & monitoring
│   ├── utilities_agent.py      # Data Plane - electricity/gas setup
│   └── broadband_agent.py      # Data Plane - internet setup
├── bus/                        # Message bus layer
│   ├── __init__.py
│   └── redis_bus.py           # Redis Pub/Sub implementation
├── core/                       # Shared components
│   ├── __init__.py
│   ├── schemas.py             # Pydantic event models
│   ├── logger.py              # Structured logging
│   └── llm_parser.py          # LLM input parser
├── api/                        # API layer
│   ├── __init__.py
│   └── main.py                # FastAPI application
├── benchmark/                  # Performance testing
│   ├── __init__.py
│   └── benchmark.py           # Centralized vs Hybrid comparison
├── tests/                      # Test suite
│   ├── __init__.py
│   └── test_agents.py         # Async agent unit tests
├── .env.example               # Environment variables template
├── .gitignore                 # Git ignore rules
├── .python-version            # Python version specification
├── pyproject.toml             # Project dependencies
├── uv.lock                    # Dependency lock file
└── README.md                  # This file
```

## Getting Started

### Prerequisites

- Python 3.12 or higher
- Redis 7.2 or higher
- uv package manager

### Installation

1. Clone the repository
   ```bash
   git clone https://github.com/22CB006/hybrid-multi-agent-orchestration.git
   cd hybrid-multi-agent-orchestration
   ```

2. Install dependencies
   ```bash
   uv sync
   ```

3. Set up environment variables
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

4. Start Redis
   ```bash
   redis-server
   ```

5. Run the application
   ```bash
   uv run python api/main.py
   ```

### Quick Start Example

```python
# Example: User request for utilities and broadband setup
# Input: "Help me set up utilities and broadband for my new flat"

# System Flow:
# 1. User → Main Agent (parse intent with LLM)
# 2. Main Agent → Bus (publish parallel tasks)
# 3. Utilities Agent + Broadband Agent execute concurrently
# 4. Agents publish completion events
# 5. Main Agent monitors and aggregates results
# 6. Main Agent → User (consolidated response)
```

## Testing

Run the test suite:

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=agents --cov=bus --cov=core

# Run specific test file
uv run pytest tests/test_agents.py -v
```

## Benchmarking

Compare centralized vs hybrid architecture performance:

```bash
uv run python benchmark/benchmark.py
```

The benchmark simulates 100 move setup workflows and measures:
- Average latency per workflow
- System throughput (workflows/sec)
- Message hop count
- Main Agent CPU load

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| REDIS_HOST | Redis server hostname | localhost |
| REDIS_PORT | Redis server port | 6379 |
| LLM_MODEL | LLM model for input parsing | llama3 |
| LLM_API_KEY | API key for LLM service | - |
| LOG_LEVEL | Logging level | INFO |
| ENVIRONMENT | Environment name | development |

### Message Event Schema

All events conform to a shared schema:

```json
{
  "event_id": "evt_a3f7b9c2",
  "task_id": "task_123",
  "event_type": "TASK_COMPLETED",
  "agent_name": "utilities_agent",
  "timestamp": "2025-01-15T10:01:22Z",
  "payload": {
    "service": "electricity",
    "provider": "Octopus Energy",
    "status": "activated"
  },
  "metadata": {
    "retry_count": 0,
    "origin": "data_plane",
    "schema_version": "1.0"
  }
}
```

## Failure Handling

| Scenario | Strategy | Owner |
|----------|----------|-------|
| Agent task fails once | Auto-retry (3 attempts, exponential backoff) | Agent |
| All retries exhausted | Publish to dead-letter queue | Agent |
| Dead-letter event | Log, escalate, retry with fallback | Main Agent |
| Agent stops heartbeat | Remove from registry, re-route tasks | Main Agent |
| Message bus unavailable | Local buffer with TTL, retry on reconnect | Agent |
| Duplicate event | Idempotency key prevents re-execution | All Agents |

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Language | Python 3.12 | Core implementation |
| API Framework | FastAPI | REST API endpoints |
| Message Bus | Redis Pub/Sub | Event-driven communication |
| Validation | Pydantic v2 | Type-safe event schemas |
| LLM | LLaMA 3 | Natural language input parsing |
| Testing | pytest + pytest-asyncio | Async test support |
| Package Manager | uv | Fast dependency management |

## Scalability Roadmap

| Phase | Scale | Evolution |
|-------|-------|-----------|
| Phase 1 (MVP) | 2 agents, 100 workflows | Redis Pub/Sub + Python asyncio |
| Phase 2 (Growth) | 10+ agents, 10K workflows/day | Migrate to NATS or Kafka |
| Phase 3 (Platform) | 50+ agents, millions/year | Service mesh, distributed tracing, Kubernetes |
| Phase 4 (Enterprise) | Full ecosystem | Event sourcing, CQRS, multi-region |

## Author

Arya Lakshmi M - Architecture Design Review for Just Move In

GitHub: [@22CB006](https://github.com/22CB006)
