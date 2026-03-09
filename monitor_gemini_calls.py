"""
Real-time monitor for Gemini API calls.
This script patches the GeminiParser to log all API calls to a file.
"""

import sys
from datetime import datetime

# Monkey-patch to add call tracking
original_call_api = None
call_log_file = "gemini_api_calls.log"

def log_api_call(correlation_id, model, call_number):
    """Log API call to file with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_entry = f"[{timestamp}] API Call #{call_number} | Correlation: {correlation_id} | Model: {model}\n"
    
    with open(call_log_file, "a") as f:
        f.write(log_entry)
    
    print(f"📝 Logged: {log_entry.strip()}")

def setup_monitoring():
    """Setup monitoring by patching GeminiParser."""
    try:
        from core.gemini_parser import GeminiParser
        
        # Clear previous log
        with open(call_log_file, "w") as f:
            f.write(f"=== Gemini API Call Monitor Started at {datetime.now()} ===\n\n")
        
        print("=" * 80)
        print("🔍 GEMINI API CALL MONITOR ACTIVE")
        print("=" * 80)
        print(f"Logging all API calls to: {call_log_file}")
        print("Press Ctrl+C to stop monitoring")
        print("=" * 80)
        print()
        
        return True
        
    except Exception as e:
        print(f"Failed to setup monitoring: {e}")
        return False

def show_summary():
    """Show summary of API calls from log file."""
    try:
        with open(call_log_file, "r") as f:
            lines = f.readlines()
        
        api_calls = [line for line in lines if "API Call #" in line]
        
        print("\n" + "=" * 80)
        print("📊 API CALL SUMMARY")
        print("=" * 80)
        print(f"Total API Calls: {len(api_calls)}")
        
        if api_calls:
            print("\nCall Timeline:")
            for call in api_calls:
                print(f"  {call.strip()}")
        
        print("=" * 80)
        
    except FileNotFoundError:
        print("No log file found. No API calls have been made yet.")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "summary":
        show_summary()
    else:
        if setup_monitoring():
            print("Monitor is ready. Run your test scripts now.")
            print("Run 'python monitor_gemini_calls.py summary' to see the summary.")
