import urllib.request
import urllib.parse
import json
import time
import sys

BASE_URL = "http://127.0.0.1:5000"

def call_api(method, path, data=None):
    url = f"{BASE_URL}{path}"
    print(f"\n" + "="*60)
    print(f"🚀 REQUEST: {method} {url}")
    if data:
        print(f"📦 PAYLOAD: {json.dumps(data, indent=2)}")
    
    try:
        start_time = time.time()
        
        # Prepare request
        req_data = json.dumps(data).encode('utf-8') if data else None
        req = urllib.request.Request(url, data=req_data, method=method)
        if data:
            req.add_header('Content-Type', 'application/json')
        
        # Execute request
        with urllib.request.urlopen(req) as response:
            status_code = response.getcode()
            response_text = response.read().decode('utf-8')
            
        duration = round((time.time() - start_time) * 1000, 2)
        print(f"⏱️  DURATION: {duration}ms")
        print(f"🏁 STATUS: {status_code}")
        
        if response_text:
            try:
                print(f"📄 RESPONSE:\n{json.dumps(json.loads(response_text), indent=2)}")
            except:
                print(f"📄 RESPONSE (Text):\n{response_text}")
        else:
            print("📄 RESPONSE: (Empty)")
            
        return status_code
    except urllib.error.HTTPError as e:
        print(f"⏱️  DURATION: {round((time.time() - start_time) * 1000, 2)}ms")
        print(f"🏁 STATUS: {e.code}")
        try:
            error_text = e.read().decode('utf-8')
            print(f"📄 ERROR RESPONSE:\n{error_text}")
        except:
            print(f"🚨 HTTP ERROR: {e.reason}")
        return e.code
    except Exception as e:
        print(f"🚨 EXCEPTION: {e}")
        return None

def run_all_tests():
    print("\n" + "*"*60)
    print("🔥 PROXY MAZE - LIVE END-TO-END INTEGRATION TEST (URLLIB) 🔥")
    print("*"*60)
    print(f"Target Base URL: {BASE_URL}")

    # 1. Health Check
    call_api('GET', '/health')

    # 2. Config Management
    call_api('GET', '/config')
    call_api('POST', '/config', {
        "check_interval_seconds": 45,
        "request_timeout_ms": 2500
    })

    # 3. Pool Management
    call_api('DELETE', '/proxies')
    call_api('POST', '/proxies', {
        "proxies": [
            {'id': 'p1', 'url': 'http://localhost:9001?state=healthy'},
            {'id': 'p2', 'url': 'http://localhost:9002?state=error'},
            {'id': 'p3', 'url': 'http://localhost:9003?state=timeout'}
        ],
        "replace": True
    })

    call_api('GET', '/proxies')

    # 4. Proxy Intelligence
    call_api('GET', '/proxies/p1')
    call_api('GET', '/proxies/p1/history')

    # 5. Monitoring
    call_api('GET', '/metrics')
    call_api('GET', '/alerts')

    # 6. Webhooks
    call_api('POST', '/webhooks', {
        "url": "https://webhook.site/test-endpoint", 
        "events": ["alert.fired"],
        "integration_type": "generic"
    })

    # 7. Management & Inspection
    call_api('GET', '/management/scheduler/status')
    
    call_api('PUT', '/management/config', {
        "check_interval_seconds": 15,
        "request_timeout_ms": 3000
    })
    
    call_api('POST', '/management/purge', {
        "days_to_keep": 0
    })

    print("\n" + "="*60)
    print("✅ LIVE INTEGRATION TEST COMPLETE")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_all_tests()
