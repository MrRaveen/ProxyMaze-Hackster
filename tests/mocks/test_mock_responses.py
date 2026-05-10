import httpx
import subprocess
import time
import sys
import os

def run_tests():
    # Start the mock servers in the background
    base_dir = os.path.dirname(os.path.abspath(__file__))
    runner_script = os.path.join(base_dir, 'run_mocks.py')
    
    print("Starting mock servers...")
    process = subprocess.Popen(
        [sys.executable, runner_script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for servers to initialize
    time.sleep(3)
    
    ports = [8001, 8002, 8003]
    success_count = 0
    total_tests = 0
    
    try:
        with httpx.Client(timeout=10.0) as client:
            for port in ports:
                base_url = f"http://localhost:{port}/proxy/test_id"
                
                # Test 1: Healthy State
                print(f"[{port}] Testing Healthy State...")
                total_tests += 1
                resp = client.get(f"{base_url}?state=healthy")
                if resp.status_code == 200 and resp.json()['status'] == 'success':
                    print(f"  ✅ Port {port} Healthy: PASS")
                    success_count += 1
                else:
                    print(f"  ❌ Port {port} Healthy: FAIL ({resp.status_code})")
                
                # Test 2: Error State
                print(f"[{port}] Testing Error State...")
                total_tests += 1
                resp = client.get(f"{base_url}?state=error")
                if resp.status_code == 503 and resp.json()['status'] == 'error':
                    print(f"  ✅ Port {port} Error: PASS")
                    success_count += 1
                else:
                    print(f"  ❌ Port {port} Error: FAIL ({resp.status_code})")
                
                # Test 3: Timeout State
                print(f"[{port}] Testing Timeout State (expecting ~5s delay)...")
                total_tests += 1
                start_time = time.time()
                resp = client.get(f"{base_url}?state=timeout")
                duration = time.time() - start_time
                if resp.status_code == 200 and duration >= 5.0:
                    print(f"  ✅ Port {port} Timeout: PASS ({duration:.2f}s)")
                    success_count += 1
                else:
                    print(f"  ❌ Port {port} Timeout: FAIL ({resp.status_code}, {duration:.2f}s)")
                    
    except Exception as e:
        print(f"Test execution error: {e}")
    finally:
        print("Shutting down mock servers...")
        process.terminate()
        process.wait()
        
    print(f"\nFinal Results: {success_count}/{total_tests} tests passed.")
    if success_count == total_tests:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == '__main__':
    run_tests()
