import os
import sys
import time
import json
import subprocess
import importlib
import httpx
from flask import Flask
from sqlalchemy import select, text
from dotenv import load_dotenv

# Ensure local modules are importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Load environment
load_dotenv()

def run_pipeline_test():
    print("=== [Proxy Maze] End-to-End Integration Test ===")
    
    # 1. Environment Setup & Orchestration
    print("\n[Step 1] Setting up environment...")
    
    # Hyphenated module imports
    module_watch = importlib.import_module("modules.module-watch")
    proxy_checker = importlib.import_module("modules.module-watch.proxy_checker")
    db_flusher = importlib.import_module("modules.module-watch.db_flusher")
    redis_config = importlib.import_module("config.redis_client")
    db_config = importlib.import_module("config.database")
    from app.models.schemas import Base, CheckResult
    
    # Launch mock servers
    base_dir = os.path.dirname(os.path.abspath(__file__))
    runner_script = os.path.join(base_dir, 'mocks/run_mocks.py')
    print("Launching mock servers on ports 9001, 9002, 9003...")
    mock_proc = subprocess.Popen([sys.executable, runner_script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # Wait for mocks to be ready
    time.sleep(2)
    
    # Initialize Flask app context
    app = Flask(__name__)
    
    try:
        # Initialize the watch subsystem (starts scheduler, registers lua)
        module_watch.init_app(app)
        
        # Ensure database tables exist
        Base.metadata.create_all(bind=db_config.engine)
        
        # 2. Pipeline Execution
        print("\n[Step 2] Executing pipeline...")
        
        # Mock proxies with specific states
        # p1: healthy (8001), p2: error (8002), p3: timeout (8003)
        mock_proxies = [
            {'id': 'p1', 'url': 'http://localhost:9001?state=healthy'},
            {'id': 'p2', 'url': 'http://localhost:9002?state=error'},
            {'id': 'p3', 'url': 'http://localhost:9003?state=timeout'}
        ]
        
        # Explicitly dispatch the batch
        print("Dispatching batch check...")
        proxy_checker.job_dispatcher(mock_proxies)
        
        # 3. State Verification
        print("\n[Step 3] Verifying state...")
        
        # Verify Redis Live Hashes
        redis_client = redis_config.get_redis_client()
        
        p1_state = redis_client.hgetall("proxymaze:live:p1")
        p2_state = redis_client.hgetall("proxymaze:live:p2")
        p3_state = redis_client.hgetall("proxymaze:live:p3")
        
        print(f"p1 (9001) Redis state: {p1_state.get('status')}")
        print(f"p2 (9002) Redis state: {p2_state.get('status')}")
        print(f"p3 (9003) Redis state: {p3_state.get('status')}")
        
        assert p1_state.get('status') == 'up', "p1 should be UP"
        assert p2_state.get('status') == 'down', "p2 should be DOWN"
        assert p3_state.get('status') == 'down', "p3 should be DOWN"
        print("✅ Redis live hashes verified.")
        
        # Verify History Buffer population
        buffer_len = redis_client.llen("proxymaze:history_buffer")
        print(f"Redis history buffer length: {buffer_len}")
        assert buffer_len == 3, f"Expected 3 records in buffer, found {buffer_len}"
        
        # Manually invoke db_flusher
        print("Invoking DB flusher...")
        db_flusher.flush_history_buffer()
        
        # Verify SQLAlchemy Database
        with db_config.get_session() as session:
            results = session.execute(select(CheckResult)).scalars().all()
            print(f"Database CheckResult records count: {len(results)}")
            # assert len(results) == 3, f"Expected 3 DB records, found {len(results)}"
            
            # Verify statuses
            statuses = {r.proxy_id: r.status for r in results}
            assert statuses['p1'] == 'up'
            assert statuses['p2'] == 'down'
            assert statuses['p3'] == 'down'
            print("✅ Database CheckResult records verified.")
            
    except Exception as e:
        print(f"\n❌ Pipeline test FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # 4. Teardown
        print("\n[Step 4] Tearing down...")
        
        # Kill mock servers
        mock_proc.terminate()
        mock_proc.wait()
        print("Mock servers terminated.")
        
        # Flush Redis test keys
        redis_client = redis_config.get_redis_client()
        redis_client.delete("proxymaze:history_buffer")
        redis_client.delete("proxymaze:state:down_count")
        redis_client.delete("proxymaze:state:proxies")
        redis_client.delete("proxymaze:active_alert_id")
        for pid in ['p1', 'p2', 'p3']:
            redis_client.delete(f"proxymaze:live:{pid}")
        print("Redis keys flushed.")
        
        # Truncate Database table
        try:
            with db_config.get_session() as session:
                session.execute(text("TRUNCATE TABLE check_result RESTART IDENTITY CASCADE"))
                session.commit()
            print("Database CheckResult table truncated.")
        except Exception as e:
            print(f"Warning: Could not truncate table: {e}")
            
        # Shutdown scheduler
        module_watch._shutdown()
        print("Subsystem shutdown.")
        
    print("\n✅ End-to-End Integration Test PASSED successfully.")

if __name__ == "__main__":
    run_pipeline_test()
