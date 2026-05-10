import subprocess
import sys
import os
import signal
import time

def run_instances():
    ports = [9001, 9002, 9003]
    processes = []
    
    # Get absolute path to the proxy_mock script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    mock_script = os.path.join(base_dir, 'proxy_mock.py')
    
    print(f"Starting {len(ports)} mock proxy instances...")
    
    try:
        for port in ports:
            print(f"Launching instance on port {port}...")
            # Use sys.executable to ensure we use the same Python environment
            process = subprocess.Popen(
                [sys.executable, mock_script, str(port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            processes.append(process)
            
        print("All instances started. Press Ctrl+C to stop.")
        
        # Keep the script running to monitor processes
        while True:
            time.sleep(1)
            for p in processes:
                if p.poll() is not None:
                    port_num = ports[processes.index(p)]
                    # Read the trapped error message
                    error_output = p.stderr.read() 
                    print(f"Process on port {port_num} exited unexpectedly.")
                    print(f"--- Error Details ---\n{error_output}")
                    raise KeyboardInterrupt
                    
    except KeyboardInterrupt:
        print("\nStopping mock instances...")
    finally:
        for p in processes:
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        print("All instances stopped.")

if __name__ == '__main__':
    run_instances()
