import os
import sys
import time
import json
import uuid
import multiprocessing
from flask import Flask, request as flask_request, jsonify
from sqlalchemy import select, text
from dotenv import load_dotenv
from multiprocessing import Manager

# Ensure local modules are importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

def run_mock_receiver(received_list, port, return_503_count=0):
    """
    Runs a mock webhook receiver Flask app.
    """
    app = Flask(__name__)
    
    # Use a dictionary to maintain state across requests
    # In Flask, we can use a global or a closure
    class State:
        retries_left = return_503_count
    
    state = State()

    @app.route('/webhook', methods=['POST'])
    def handle_webhook():
        if state.retries_left > 0:
            state.retries_left -= 1
            print(f"[Mock Receiver] Returning 503 Service Unavailable (Attempts left: {state.retries_left})")
            return "Service Unavailable", 503
            
        data = flask_request.get_json()
        print(f"[Mock Receiver] Received payload: {data}")
        received_list.append(data)
        return jsonify({"status": "ok"}), 200

    # Run without output to keep logs clean
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    app.run(port=port, debug=False, use_reloader=False)

def test_alert_delivery():
    print("=== [Proxy Maze] Alert Delivery & Resilience Test ===")
    
    # Import modules dynamically to ensure environment is set
    import importlib
    module_watch = importlib.import_module("modules.module-watch")
    proxy_checker = importlib.import_module("modules.module-watch.proxy_checker")
    redis_config = importlib.import_module("config.redis_client")
    db_config = importlib.import_module("config.database")
    from app.models.schemas import Base, Alert, Webhook, WebhookDelivery
    
    manager = Manager()
    received_requests = manager.list()
    
    # 1. Start Mock Webhook Receiver
    # We configure it to fail twice with 503 to test the exponential backoff
    mock_port = 9005
    receiver_proc = multiprocessing.Process(
        target=run_mock_receiver, 
        args=(received_requests, mock_port, 2), 
        daemon=True
    )
    receiver_proc.start()
    print(f"Launched mock webhook receiver on port {mock_port} (with 2 initial 503 failures)")
    time.sleep(2) # Give Flask time to bind
    
    # Set unique group ID for this test run to avoid picking up old messages
    test_group_id = f"test-group-{uuid.uuid4().hex[:8]}"
    os.environ['KAFKA_GROUP_ID'] = test_group_id
    print(f"Using Kafka Consumer Group: {test_group_id}")
    
    app = Flask(__name__)
    
    try:
        # Reset State
        print("\n[Step 1] Cleaning up state and preparing database...")
        redis_client = redis_config.get_redis_client()
        redis_client.flushall()
        
        with db_config.get_session() as session:
            # Drop and recreate tables to ensure fresh schema and no data
            Base.metadata.drop_all(bind=db_config.engine)
            Base.metadata.create_all(bind=db_config.engine)
            
            # Specifically truncate just in case drop/create didn't clear everything (SQLAlchemy quirks)
            session.execute(text("TRUNCATE TABLE alert, webhook, webhook_delivery RESTART IDENTITY CASCADE"))
            session.commit()
            
            # Register a webhook for alert events
            webhook = Webhook(
                url=f"http://localhost:{mock_port}/webhook",
                events=["alert.fired"]
            )
            session.add(webhook)
            session.commit()
            print(f"✅ Webhook registered in DB: {webhook.url}")

        # Initialize the Watch subsystem (this also spawns the Kafka delivery process)
        print("\n[Step 2] Initializing Watch subsystem...")
        module_watch.init_app(app)
        
        # 2. Simulate Threshold Breach
        # A pool of 1 with 1 failure = 100% failure rate (> 20% threshold)
        print("\n[Step 3] Simulating threshold breach (1/1 proxies down)...")
        # We need to make sure the mock server at 9001 returns an error for p1
        # But for this test, proxy_worker just needs to return 'down'
        # We can bypass the actual network check by calling it with an invalid URL or just rely on the 9001 mock
        
        proxy_checker.proxy_worker({'id': 'p1', 'url': 'http://localhost:9999/fail'}, total_pool_size=1)
        
        # 3. Pipeline Verification
        print("\n[Step 4] Verifying end-to-end delivery with retries...")
        
        # The consumer should:
        # 1. Pick up alert.fired from Kafka
        # 2. Attempt delivery to 9005 -> Get 503
        # 3. Retry after 1s -> Get 503
        # 4. Retry after 2s -> Get 200 SUCCESS
        
        start_time = time.time()
        max_wait = 40 # Contract is 60s, we expect success within ~10s
        found = False
        
        while time.time() - start_time < max_wait:
            if len(received_requests) > 0:
                found = True
                break
            time.sleep(1)
            
        assert found, f"Webhook was not delivered within {max_wait}s"
        
        payload = received_requests[0]
        alert_id = payload.get('alert_id')
        print(f"✅ Webhook received successfully! Alert ID: {alert_id}")
        assert alert_id is not None
        assert payload.get('failure_rate') == 1.0
        assert 'p1' in payload.get('failed_proxy_ids')
        
        # 4. Database Persistence Verification
        print("\n[Step 5] Verifying database records...")
        with db_config.get_session() as session:
            # Check Alert Table
            alert_rec = session.execute(select(Alert).where(Alert.alert_id == alert_id)).scalars().first()
            assert alert_rec is not None, "Alert record not found in DB"
            assert alert_rec.status == 'active', "Alert status should be 'active'"
            print(f"✅ Database Alert record verified: {alert_rec.alert_id} [Status: {alert_rec.status}]")
            
            # Check WebhookDelivery Table
            deliveries = session.execute(select(WebhookDelivery)).scalars().all()
            print(f"Delivery logs in DB: {len(deliveries)}")
            # We expect 3 logs: 2 failures (False) and 1 success (True)
            successes = [d for d in deliveries if d.success]
            failures = [d for d in deliveries if not d.success]
            
            assert len(successes) >= 1, "Successful delivery not logged in DB"
            assert len(failures) >= 2, f"Expected at least 2 retry failure logs, found {len(failures)}"
            print("✅ WebhookDelivery audit trail verified (includes retry attempts).")
            
    except Exception as e:
        print(f"\n❌ Alert delivery test FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("\n[Step 6] Cleaning up...")
        if 'receiver_proc' in locals():
            receiver_proc.terminate()
            receiver_proc.join()
        if 'module_watch' in locals():
            module_watch._shutdown()
        print("✅ Alert delivery integration test complete.")

if __name__ == "__main__":
    test_alert_delivery()
