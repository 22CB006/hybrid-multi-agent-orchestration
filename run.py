"""
Run the full hybrid multi-agent orchestration system.

Starts all components concurrently in a single event loop:
1. Utilities Agent (data plane)
2. Broadband Agent (data plane)
3. FastAPI server with Main Agent (control plane + API)
"""

import asyncio
import signal

import uvicorn

from agents.broadband_agent import BroadbandAgent
from agents.utilities_agent import UtilitiesAgent
from core.config import Config


async def main():
    print("[run] Starting Hybrid Multi-Agent Orchestration System")
    print("[run] API server: http://localhost:8000")
    print("[run] API docs:   http://localhost:8000/docs")
    print()

    config = Config()

    # Start data plane agents
    utilities = UtilitiesAgent(config)
    broadband = BroadbandAgent(config)

    await utilities.start()
    print("[run] Utilities Agent started")
    await broadband.start()
    print("[run] Broadband Agent started")

    # Start API server (runs Main Agent internally via lifespan)
    server_config = uvicorn.Config(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
    server = uvicorn.Server(server_config)

    # Handle shutdown
    stop = asyncio.Event()
    
    # Set up signal handlers (works on Unix, not Windows)
    import sys
    if sys.platform != "win32":
        loop = asyncio.get_event_loop()
        for sig_name in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig_name, stop.set)

    # Run server
    print("[run] Starting API server...")
    server_task = asyncio.create_task(server.serve())

    # Wait for shutdown signal or server to stop
    try:
        await server_task
    except KeyboardInterrupt:
        print("\n[run] Shutting down...")
        server.should_exit = True
        await utilities.shutdown()
        await broadband.shutdown()
        print("[run] Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
