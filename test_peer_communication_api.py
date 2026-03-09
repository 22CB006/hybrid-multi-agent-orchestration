"""Test script for peer-to-peer communication API endpoints.

This script demonstrates how to use the API to test direct agent communication.
"""

import requests
import time
import json


BASE_URL = "http://localhost:8000"


def print_response(title, response):
    """Pretty print API response."""
    print(f"\n{'='*70}")
    print(f"{title}")
    print(f"{'='*70}")
    print(f"Status Code: {response.status_code}")
    print(f"Response:")
    print(json.dumps(response.json(), indent=2))


def test_health():
    """Test system health."""
    print("\n🏥 Testing System Health...")
    response = requests.get(f"{BASE_URL}/health")
    print_response("Health Check", response)
    return response.status_code == 200


def test_validate_address():
    """Test address validation (triggers peer communication)."""
    print("\n📍 Testing Address Validation (Peer Communication)...")
    
    payload = {
        "address": "123 Main Street, Metro City"
    }
    
    response = requests.post(
        f"{BASE_URL}/peer-communication/validate-address",
        json=payload
    )
    
    print_response("Address Validation", response)
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Utilities Agent validated address")
        print(f"   Address: {data['address']}")
        print(f"   Electricity: {'✓' if data['electricity_available'] else '✗'}")
        print(f"   Gas: {'✓' if data['gas_available'] else '✗'}")
        print(f"   Service Area: {data['service_area']}")
        print(f"   Peer Communication: {'✓' if data['peer_communication_used'] else '✗'}")
        print(f"\n💬 {data['message']}")
    
    return response.status_code == 200


def test_check_broadband():
    """Test broadband availability check (uses peer data)."""
    print("\n🌐 Testing Broadband Availability (Uses Peer Data)...")
    
    payload = {
        "address": "123 Main Street, Metro City"
    }
    
    response = requests.post(
        f"{BASE_URL}/peer-communication/check-broadband",
        json=payload
    )
    
    print_response("Broadband Availability", response)
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Broadband Agent checked availability")
        print(f"   Address: {data['address']}")
        print(f"   Fiber: {'✓' if data['fiber_available'] else '✗'}")
        print(f"   Cable: {'✓' if data['cable_available'] else '✗'}")
        print(f"   Max Speed: {data['max_speed_mbps']} Mbps")
        print(f"   Used Utilities Data: {'✓' if data['utilities_validated'] else '✗'}")
        if data['service_area']:
            print(f"   Service Area (from Utilities): {data['service_area']}")
        print(f"\n💬 {data['message']}")
    
    return response.status_code == 200


def test_complete_workflow():
    """Test complete peer communication workflow."""
    print("\n🔄 Testing Complete Peer Communication Workflow...")
    
    payload = {
        "address": "456 Peer Street, Tech City"
    }
    
    response = requests.post(
        f"{BASE_URL}/peer-communication/test-workflow",
        json=payload
    )
    
    print_response("Complete Workflow", response)
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Workflow completed successfully")
        print(f"   Correlation ID: {data['correlation_id']}")
        print(f"   Address: {data['address']}")
        print(f"   Status: {data['workflow_status']}")
        
        print(f"\n📋 Workflow Steps:")
        for step in data['steps']:
            print(f"   {step['step']}. {step['agent'].upper()}: {step['action']}")
            print(f"      Status: {step['status']}")
            print(f"      Peer Communication: {step['peer_communication']}")
        
        print(f"\n💬 {data['message']}")
        print(f"\n📝 Note: {data['note']}")
    
    return response.status_code == 200


def main():
    """Run all tests."""
    print("="*70)
    print("PEER-TO-PEER COMMUNICATION API TESTS")
    print("="*70)
    print("\nMake sure the API is running:")
    print("  uvicorn api.main:app --reload")
    print("\nAnd that Redis is running:")
    print("  redis-server")
    print("\nAnd that agents are running:")
    print("  python agents/utilities_agent.py")
    print("  python agents/broadband_agent.py")
    
    input("\nPress Enter to start tests...")
    
    results = []
    
    # Test 1: Health Check
    results.append(("Health Check", test_health()))
    time.sleep(1)
    
    # Test 2: Validate Address (triggers peer communication)
    results.append(("Address Validation", test_validate_address()))
    time.sleep(2)  # Give time for peer communication
    
    # Test 3: Check Broadband (uses peer data)
    results.append(("Broadband Check", test_check_broadband()))
    time.sleep(1)
    
    # Test 4: Complete Workflow
    results.append(("Complete Workflow", test_complete_workflow()))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name:.<50} {status}")
    
    total = len(results)
    passed = sum(1 for _, p in results if p)
    
    print("="*70)
    print(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        print("\nTo see peer communication in action:")
        print("1. Check the agent logs for 'direct peer communication' messages")
        print("2. Look for 'Published address validation to Broadband Agent'")
        print("3. Look for 'Received address validation from Utilities Agent'")
    else:
        print("\n⚠️  Some tests failed. Check the API and agent logs.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTests interrupted by user")
    except requests.exceptions.ConnectionError:
        print("\n❌ ERROR: Could not connect to API")
        print("Make sure the API is running:")
        print("  uvicorn api.main:app --reload")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
