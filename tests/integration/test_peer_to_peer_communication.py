"""Integration tests for direct peer-to-peer agent communication.

Tests the address validation scenario where:
1. Utilities Agent validates an address
2. Utilities publishes AddressValidated event DIRECTLY to Broadband's channel
3. Broadband Agent receives and uses the validation
4. Main Agent observes but doesn't route

This demonstrates true peer-to-peer communication without Main Agent routing.
"""

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import redis.asyncio as redis

from agents.broadband_agent import BroadbandAgent
from agents.utilities_agent import UtilitiesAgent
from bus.redis_bus import RedisBus
from core.config import Config
from core.policy_enforcer import PolicyEnforcer
from core.schemas import TaskRequest, AddressValidated


@pytest.mark.asyncio
async def test_direct_address_validation_communication(test_config):
    """Test Utilities Agent sending address validation directly to Broadband Agent.
    
    Flow:
    1. Start both agents
    2. Send validate_address task to Utilities
    3. Utilities validates and publishes AddressValidated to Broadband's channel
    4. Broadband receives validation directly
    5. Broadband uses validation data in availability check
    """
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)
    
    # Monitor bus to capture all events
    monitor_bus = RedisBus(test_config, utilities_agent.logger)
    await monitor_bus.connect()
    
    captured_events = []
    
    async def capture_all_events(event):
        captured_events.append(event)
    
    await monitor_bus.subscribe("agent.*.*", capture_all_events)
    
    try:
        # Start both agents
        await utilities_agent.start()
        await broadband_agent.start()
        
        # Give agents time to subscribe
        await asyncio.sleep(0.2)
        
        # Step 1: Send address validation request to Utilities Agent
        test_address = "123 Main Street, Metro City"
        correlation_id = uuid4()
        
        validation_request = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="test",
            target_agent="utilities",
            task_type="validate_address",
            payload={"address": test_address},
            timeout_seconds=10,
        )
        
        await monitor_bus.publish("agent.utilities.task_request", validation_request)
        
        # Wait for processing
        await asyncio.sleep(0.5)
        
        # Step 2: Verify AddressValidated event was published to Broadband's channel
        address_validated_events = [
            e for e in captured_events
            if getattr(e, 'event_type', None) == 'address_validated'
            and getattr(e, 'address', None) == test_address
        ]
        
        assert len(address_validated_events) > 0, "AddressValidated event should be published"
        
        validation_event = address_validated_events[0]
        assert validation_event.source_agent == "utilities"
        assert validation_event.address == test_address
        assert validation_event.electricity_available is True
        assert validation_event.gas_available is True
        
        # Step 3: Verify Broadband Agent received and stored the validation
        assert test_address in broadband_agent.validated_addresses
        stored_validation = broadband_agent.validated_addresses[test_address]
        assert stored_validation["validated_by"] == "utilities"
        assert stored_validation["electricity_available"] is True
        assert stored_validation["service_area"] == "metro"
        
        # Step 4: Send availability check to Broadband and verify it uses validation data
        availability_request = TaskRequest(
            correlation_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source_agent="test",
            target_agent="broadband",
            task_type="check_availability",
            payload={"address": test_address},
            timeout_seconds=10,
        )
        
        await monitor_bus.publish("agent.broadband.task_request", availability_request)
        
        # Wait for processing
        await asyncio.sleep(0.5)
        
        # Find the response
        from core.schemas import TaskResponse
        broadband_responses = [
            e for e in captured_events
            if isinstance(e, TaskResponse) and e.source_agent == "broadband"
        ]
        
        assert len(broadband_responses) > 0, "Broadband should respond"
        
        response = broadband_responses[0]
        result = response.result
        
        # Verify response includes utilities validation data
        assert result["utilities_validated"] is True
        assert result["service_area"] == "metro"
        assert result["electricity_available"] is True
        assert result["gas_available"] is True
        
        print("\n✅ Direct peer-to-peer communication successful!")
        print(f"   Utilities validated address: {test_address}")
        print(f"   Broadband received validation directly (no Main Agent routing)")
        print(f"   Broadband used validation in availability check")
        
    finally:
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await monitor_bus.disconnect()


@pytest.mark.asyncio
async def test_policy_allows_peer_communication(test_config):
    """Test that PolicyEnforcer allows the specific peer communication channel.
    
    Verifies:
    1. Utilities → agent.broadband.address_validated is ALLOWED
    2. Utilities → agent.broadband.task_request is BLOCKED
    """
    redis_client = await redis.from_url(
        test_config.redis_url, encoding="utf-8", decode_responses=True
    )
    
    try:
        policy_enforcer = PolicyEnforcer(redis_client, test_config)
        
        # Test 1: Allowed peer channel (address_validated)
        allowed_event = AddressValidated(
            correlation_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source_agent="utilities",
            address="123 Test St",
            electricity_available=True,
            gas_available=True,
            service_area="metro",
            estimated_connection_days=3,
        )
        
        is_allowed, reason = await policy_enforcer.check_channel_isolation(
            allowed_event,
            "agent.broadband.address_validated"
        )
        
        assert is_allowed is True, f"Peer communication should be allowed: {reason}"
        print("\n✅ Policy allows: utilities → agent.broadband.address_validated")
        
        # Test 2: Blocked input channel (task_request)
        blocked_event = TaskRequest(
            correlation_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source_agent="utilities",
            target_agent="broadband",
            task_type="check_availability",
            payload={"address": "123 Test St"},
        )
        
        is_allowed, reason = await policy_enforcer.check_channel_isolation(
            blocked_event,
            "agent.broadband.task_request"
        )
        
        assert is_allowed is False, "Input channel access should be blocked"
        assert "Channel isolation violation" in reason
        print("✅ Policy blocks: utilities → agent.broadband.task_request")
        
        # Test 3: Verify allowed channels configuration
        assert "utilities" in policy_enforcer.allowed_peer_channels
        assert "agent.broadband.address_validated" in policy_enforcer.allowed_peer_channels["utilities"]
        print("✅ Policy configuration includes peer communication rules")
        
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_no_main_agent_required_for_peer_communication(test_config):
    """Test that peer communication works WITHOUT Main Agent running.
    
    This proves true peer-to-peer communication:
    - Only Utilities and Broadband agents running
    - No Main Agent
    - Communication still works
    """
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)
    
    # Monitor bus (simulating external observer, NOT Main Agent)
    monitor_bus = RedisBus(test_config, utilities_agent.logger)
    await monitor_bus.connect()
    
    captured_events = []
    
    async def capture_all_events(event):
        captured_events.append(event)
    
    await monitor_bus.subscribe("agent.*.*", capture_all_events)
    
    try:
        # Start ONLY data plane agents (NO Main Agent)
        await utilities_agent.start()
        await broadband_agent.start()
        
        await asyncio.sleep(0.2)
        
        # Send validation request
        test_address = "456 Peer Street"
        validation_request = TaskRequest(
            correlation_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source_agent="test",
            target_agent="utilities",
            task_type="validate_address",
            payload={"address": test_address},
            timeout_seconds=10,
        )
        
        await monitor_bus.publish("agent.utilities.task_request", validation_request)
        
        await asyncio.sleep(0.5)
        
        # Verify communication happened
        address_validated_events = [
            e for e in captured_events
            if getattr(e, 'event_type', None) == 'address_validated'
            and getattr(e, 'address', None) == test_address
        ]
        
        assert len(address_validated_events) > 0, "Peer communication should work without Main Agent"
        
        # Verify Broadband received it
        assert test_address in broadband_agent.validated_addresses
        
        # Verify NO Main Agent events (proves it wasn't involved)
        main_agent_events = [
            e for e in captured_events
            if hasattr(e, 'source_agent') and e.source_agent == "main"
        ]
        
        assert len(main_agent_events) == 0, "Main Agent should not be involved"
        
        print("\n✅ Peer communication works WITHOUT Main Agent!")
        print(f"   Utilities → Broadband communication successful")
        print(f"   No Main Agent events detected")
        print(f"   True peer-to-peer architecture confirmed")
        
    finally:
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await monitor_bus.disconnect()


@pytest.mark.asyncio
async def test_complete_address_validation_workflow(test_config):
    """Test complete workflow: User moves to new address, both services validate.
    
    Scenario:
    1. User provides new address
    2. Main Agent (or test) sends tasks to both Utilities and Broadband
    3. Utilities validates address first
    4. Utilities shares validation with Broadband (peer communication)
    5. Broadband uses validation in its availability check
    6. Both agents complete and respond
    """
    utilities_agent = UtilitiesAgent(test_config)
    broadband_agent = BroadbandAgent(test_config)
    
    monitor_bus = RedisBus(test_config, utilities_agent.logger)
    await monitor_bus.connect()
    
    captured_events = []
    
    async def capture_all_events(event):
        captured_events.append(event)
    
    await monitor_bus.subscribe("agent.*.*", capture_all_events)
    
    try:
        await utilities_agent.start()
        await broadband_agent.start()
        
        await asyncio.sleep(0.2)
        
        # Simulate user moving to new address
        new_address = "789 New Home Avenue"
        correlation_id = uuid4()
        
        # Send tasks to BOTH agents in parallel (like Main Agent would)
        utilities_task = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="main",
            target_agent="utilities",
            task_type="validate_address",
            payload={"address": new_address},
            timeout_seconds=10,
        )
        
        broadband_task = TaskRequest(
            correlation_id=correlation_id,
            timestamp=datetime.now(timezone.utc),
            source_agent="main",
            target_agent="broadband",
            task_type="check_availability",
            payload={"address": new_address},
            timeout_seconds=10,
        )
        
        # Send both tasks
        await monitor_bus.publish("agent.utilities.task_request", utilities_task)
        
        # Small delay to let Utilities validate first
        await asyncio.sleep(0.3)
        
        # Now send to Broadband
        await monitor_bus.publish("agent.broadband.task_request", broadband_task)
        
        # Wait for both to complete
        await asyncio.sleep(0.5)
        
        # Verify both agents completed
        from core.schemas import TaskResponse
        
        utilities_responses = [
            e for e in captured_events
            if isinstance(e, TaskResponse) and e.source_agent == "utilities"
        ]
        
        broadband_responses = [
            e for e in captured_events
            if isinstance(e, TaskResponse) and e.source_agent == "broadband"
        ]
        
        assert len(utilities_responses) > 0, "Utilities should complete"
        assert len(broadband_responses) > 0, "Broadband should complete"
        
        # Verify peer communication happened
        address_validated_events = [
            e for e in captured_events
            if getattr(e, 'event_type', None) == 'address_validated'
            and getattr(e, 'address', None) == new_address
        ]
        
        assert len(address_validated_events) > 0, "Peer communication should occur"
        
        # Verify Broadband used the validation
        broadband_result = broadband_responses[0].result
        assert broadband_result["utilities_validated"] is True
        
        print("\n✅ Complete workflow successful!")
        print(f"   Address: {new_address}")
        print(f"   Utilities validated: ✓")
        print(f"   Peer communication: ✓")
        print(f"   Broadband used validation: ✓")
        print(f"   Both agents completed: ✓")
        
    finally:
        await utilities_agent.shutdown()
        await broadband_agent.shutdown()
        await monitor_bus.disconnect()


if __name__ == "__main__":
    """Run tests directly."""
    import sys
    
    async def run_tests():
        config = Config()
        
        print("=" * 70)
        print("PEER-TO-PEER AGENT COMMUNICATION TESTS")
        print("=" * 70)
        
        print("\n[Test 1] Direct Address Validation Communication")
        print("-" * 70)
        await test_direct_address_validation_communication(config)
        
        print("\n[Test 2] Policy Allows Peer Communication")
        print("-" * 70)
        await test_policy_allows_peer_communication(config)
        
        print("\n[Test 3] No Main Agent Required")
        print("-" * 70)
        await test_no_main_agent_required_for_peer_communication(config)
        
        print("\n[Test 4] Complete Workflow")
        print("-" * 70)
        await test_complete_address_validation_workflow(config)
        
        print("\n" + "=" * 70)
        print("ALL TESTS PASSED ✅")
        print("=" * 70)
    
    asyncio.run(run_tests())
