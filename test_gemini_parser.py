"""
Test Gemini Parser independently to verify API key and parsing functionality.

This script tests:
1. If Gemini API key is valid
2. If parsing works correctly
3. What the parsed output looks like
"""

import asyncio
from uuid import uuid4
from core.config import Config
from core.gemini_parser import GeminiParser


async def test_gemini_parser():
    """Test Gemini parser with sample inputs."""
    
    import time
    start_time = time.time()
    
    print("=" * 70)
    print("GEMINI PARSER TEST")
    print("=" * 70)
    print(f"Start Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Load configuration
    try:
        config = Config()
        print("✓ Configuration loaded successfully")
        print(f"  - Gemini Model: {config.gemini_model}")
        print(f"  - Timeout: {config.gemini_timeout}s")
        print(f"  - API Key: {config.gemini_api_key[:10]}...{config.gemini_api_key[-4:]}")
        print()
    except Exception as e:
        print(f"✗ Failed to load configuration: {e}")
        print("\nMake sure your .env file has GEMINI_API_KEY set!")
        return
    
    # Initialize parser
    try:
        parser = GeminiParser(
            api_key=config.gemini_api_key,
            model=config.gemini_model,
            timeout_seconds=10  # Longer timeout for testing
        )
        print("✓ Gemini parser initialized")
        print()
        
        # Store initial call count to track this test run
        initial_call_count = GeminiParser._total_api_calls
        
    except Exception as e:
        print(f"✗ Failed to initialize parser: {e}")
        return
    
    # Test cases
    test_cases = [
        "I need electricity and broadband at 123 Main Street",
        "Setup gas and internet at 42 Oak Avenue",
        "I'm moving to 789 Pine Road and need all utilities",
        "Check if electricity is available at my new address",
    ]
    
    print("=" * 70)
    print("RUNNING TEST CASES")
    print("=" * 70)
    print()
    
    for i, user_input in enumerate(test_cases, 1):
        # Add a delay between requests to avoid rate limiting
        if i > 1:  # Skip delay for first request
            await asyncio.sleep(5)
        
        print(f"Test Case {i}:")
        print(f"Input: '{user_input}'")
        print()
        
        try:
            # Parse input
            correlation_id = uuid4()
            result = await parser.parse_input(user_input, correlation_id)
            
            # Display results
            print("✓ Parsing successful!")
            print(f"  Intent: {result.intent}")
            print(f"  Target Agents: {result.target_agents}")
            print(f"  Entities: {result.entities}")
            print(f"  Confidence: {result.confidence}")
            print()
            
        except asyncio.TimeoutError:
            print("✗ Parsing timed out (API took too long)")
            print()
            
        except Exception as e:
            print(f"✗ Parsing failed: {e}")
            print(f"  Error type: {type(e).__name__}")
            print()
            
            # Check for common errors
            if "403" in str(e) or "API key" in str(e):
                print("  → This looks like an API key issue!")
                print("  → Check your GEMINI_API_KEY in .env file")
            elif "429" in str(e):
                print("  → Rate limit exceeded, wait a moment and try again")
            elif "timeout" in str(e).lower():
                print("  → API request timed out, try increasing timeout")
            
            print()
        
        print("-" * 70)
        print()
    
    print("=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)
    print()
    
    # Print final API call statistics
    end_time = time.time()
    duration = end_time - start_time
    
    print("=" * 70)
    print("📊 GEMINI API CALL STATISTICS")
    print("=" * 70)
    print(f"   Total API Calls (All Time): {GeminiParser._total_api_calls}")
    print(f"   This Test Run: {parser.instance_api_calls}")
    print(f"   Expected Calls: {len(test_cases)}")
    if parser.instance_api_calls == len(test_cases):
        print(f"   ✅ Call count matches expected!")
    else:
        print(f"   ⚠️  WARNING: Call count mismatch!")
        print(f"   Difference: {parser.instance_api_calls - len(test_cases)} extra calls")
    print(f"   Total Duration: {duration:.2f} seconds")
    print(f"   Average per call: {duration/len(test_cases):.2f} seconds")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(test_gemini_parser())
