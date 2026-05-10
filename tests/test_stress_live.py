import os
import sys
import time
import json
import uuid
import concurrent.futures
from confluent_kafka import Consumer, KafkaError
from sqlalchemy import select, text
from dotenv import load_dotenv

# Ensure local modules are importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

def test_stress_live():
    print("=== [Proxy Maze] High-Throughput Stress Test (Live Infrastructure) ===")
    
    # 1. Initialization
    import importlib
    redis_config = importlib.import_module("config.redis_client")
    db_config = importlib.import_module("config.database")
    proxy_checker = importlib.import_module("modules.module-watch.proxy_checker")
    db_flusher = importlib.import_module("modules.module-watch.db_flusher")
    from app.models.schemas import Base, Proxy, CheckResult
    
    # We must use REAL infrastructure (no mocks for services)
    # Ensure Redis is clean
    redis_client = redis_config.get_redis_client()
    redis_client.flushall()
    print("✅ Redis flushed.")

    # Ensure DB is clean
    with db_config.get_session() as session:
        Base.metadata.drop_all(bind=db_config.engine)
        Base.metadata.create_all(bind=db_config.engine)
        
        # Seed 3 proxies in DB
        proxies_data = [
            {'id': 'p1', 'url': 'http://localhost:9001?state=healthy'},
            {'id': 'p2', 'url': 'http://localhost:9002?state=error'},
            {'id': 'p3', 'url': 'http://localhost:9003?state=timeout'}
        ]
        
        for p in proxies_data:
            session.add(Proxy(id=p['id'], url=p['url']))
        session.commit()
        print("✅ DB Seeded with 3 proxies.")

    # Set Kafka Consumer to verify payload later
    test_group = f"stress-test-{uuid.uuid4().hex[:8]}"
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': test_group,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['alert.fired'])

    # 2. Execution (Stress Test ThreadPool)
    print("\nExecuting stress test via ThreadPoolExecutor...")
    
    # We need to simulate 200 checks (rows) to test DB flusher bulk insert
    total_checks = 200
    total_pool_size = 3
    futures = []
    
    # Submit 200 tasks to the executor. 
    # Since we have 3 proxies, we just submit them round-robin.
    # 2 of 3 will fail (p2, p3), which is a ~66% failure rate, easily triggering the 20% threshold.
    start_time = time.time()
    for i in range(total_checks):
        proxy = proxies_data[i % total_pool_size]
        # Bypassing the network by mocking the `requests.get` inside the test? 
        # "STRICT CONSTRAINT: Do NOT use mocks...". 
        # If no mocks, we MUST launch the real mock servers, or let `requests` naturally fail.
        # Since p2 and p3 URLs point to localhost ports that are NOT running, 
        # they will naturally raise `requests.ConnectionError` and be marked 'down'!
        # p1 points to 9001. If it's not running, it will also fail. 
        # Let's start the actual mock servers from the `tests/mocks/run_mocks.py` script to be safe, 
        # because we need p1 to be 'up' and p2, p3 to be 'down' to match exactly "2 proxies failing".
        pass

    # Actually, let's launch the mock servers so p1 genuinely succeeds and p2, p3 genuinely fail.
    import subprocess
    base_dir = os.path.dirname(os.path.abspath(__file__))
    runner_script = os.path.join(base_dir, 'mocks/run_mocks.py')
    print("Launching real mock HTTP servers for network targets...")
    mock_proc = subprocess.Popen([sys.executable, runner_script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2) # Give mock servers time to bind
    
    # Load Lua Scripts
    from flask import Flask
    app = Flask(__name__)
    mw = importlib.import_module("modules.module-watch")
    mw.init_app(app)

    try:
        print(f"Submitting {total_checks} proxy checks to ThreadPoolExecutor...")
        for i in range(total_checks):
            proxy = proxies_data[i % total_pool_size]
            future = proxy_checker._executor.submit(proxy_checker.proxy_worker, proxy, total_pool_size)
            futures.append(future)

        # Wait for all checks to complete
        concurrent.futures.wait(futures)
        print(f"All {total_checks} checks executed in {time.time() - start_time:.2f} seconds.")

        # 3. Validation

        # Assert Redis History Buffer Length
        buffer_len = redis_client.llen("proxymaze:history_buffer")
        print(f"Redis history buffer length: {buffer_len}")
        assert buffer_len == total_checks, f"Expected {total_checks} rows in Redis buffer, found {buffer_len}"

        # Manually invoke DB Flusher
        print("\nInvoking DB Flusher for bulk insert...")
        db_flusher.flush_history_buffer()

        # Assert PostgreSQL Row Count
        with db_config.get_session() as session:
            db_count = session.query(CheckResult).count()
            print(f"PostgreSQL CheckResult rows: {db_count}")
            assert db_count == total_checks, f"Expected {total_checks} DB rows, found {db_count}"

        # Verify Kafka Payload
        print("\nVerifying Kafka alert.fired payload...")
        kafka_payload = None
        # Flush the producer so messages are actually sent
        from config.kafka_client import flush_producer
        flush_producer()

        # Poll consumer for 10 seconds to get the latest message
        poll_start = time.time()
        while time.time() - poll_start < 10:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break
            
            # Keep updating kafka_payload so we end up with the very last one
            kafka_payload = json.loads(msg.value().decode('utf-8'))
            
        assert kafka_payload is not None, "Failed to receive alert.fired from Kafka broker."
        print(f"✅ Kafka Payload received: Alert ID {kafka_payload.get('alert_id')}")
        # Failure rate might be exactly 0.666 or close to it, but definitely >= 0.20
        assert kafka_payload.get('failure_rate') >= 0.20, f"Failure rate was {kafka_payload.get('failure_rate')} which is not >= 20%"
        assert len(kafka_payload.get('failed_proxy_ids')) >= 1, "Expected at least 1 failed proxy in payload"
        # It could be p2 or p3 depending on thread execution order
        assert any(p in kafka_payload.get('failed_proxy_ids') for p in ['p2', 'p3'])

        print("\n✅ High-Throughput Stress Test PASSED.")
        
    except Exception as e:
        print(f"\n❌ Stress Test FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        print("\n[Teardown]")
        mock_proc.terminate()
        mw._shutdown()
        consumer.close()

if __name__ == "__main__":
    test_stress_live()
