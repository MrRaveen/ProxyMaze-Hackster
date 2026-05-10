import time
import json
import importlib
import os
from flask import Flask
from dotenv import load_dotenv

# Load env before any imports that might use it
load_dotenv()

def run_integration_tests():
    print("=== Module Watch Integration Tests ===")
    
    # Dynamic import for hyphenated module name
    try:
        module_watch = importlib.import_module("modules.module-watch")
        scheduler_mod = importlib.import_module("modules.module-watch.scheduler")
        pubsub_mod = importlib.import_module("modules.module-watch.pubsub_listener")
        lua_mod = importlib.import_module("modules.module-watch.lua_scripts")
        redis_config = importlib.import_module("config.redis_client")
    except ImportError as e:
        print(f"❌ Failed to import modules: {e}")
        return

    app = Flask(__name__)
    
    try:
        # 1. Initialization
        print("\n[1/4] Initializing Module Watch...")
        module_watch.init_app(app)
        print("✅ Initialization successful")

        # 2. Redis & Lua Scripts
        print("\n[2/4] Verifying Redis & Lua Scripts...")
        client = redis_config.get_redis_client()
        client.ping()
        print("✅ Redis PING: OK")
        
        # Check if script exists in the registry
        script = lua_mod.get_script('increment_and_check')
        if script:
            print(f"✅ Lua Script 'increment_and_check' registered (SHA: {script.sha})")
        else:
            print("❌ Lua Script not found")

        # 3. Scheduler Status
        print("\n[3/4] Verifying Scheduler...")
        if scheduler_mod.scheduler.running:
            print("✅ Scheduler is RUNNING")
        else:
            print("❌ Scheduler is NOT RUNNING")

        # 4. Pub/Sub Configuration Updates
        print("\n[4/4] Testing Pub/Sub Config Updates...")
        initial_interval = scheduler_mod.config_state['check_interval']
        new_interval = 45
        
        # Publish update
        update_payload = {"check_interval": new_interval}
        print(f"Publishing update: {update_payload}")
        client.publish(pubsub_mod.CONFIG_CHANNEL, json.dumps(update_payload))
        
        # Wait for async update
        print("Waiting for async update...")
        timeout = 5
        start_time = time.time()
        updated = False
        
        while time.time() - start_time < timeout:
            if scheduler_mod.config_state['check_interval'] == new_interval:
                updated = True
                break
            time.sleep(0.5)
            
        if updated:
            print(f"✅ Config updated successfully: {scheduler_mod.config_state['check_interval']}s")
        else:
            print(f"❌ Config update FAILED. State remains: {scheduler_mod.config_state['check_interval']}s")

        # 5. Job Dispatcher
        print("\n[5/5] Testing Job Dispatcher...")
        try:
            proxy_checker_mod = importlib.import_module("modules.module-watch.proxy_checker")
            mock_proxies = [
                {'id': 'p1', 'url': 'http://localhost:9001'},
                {'id': 'p2', 'url': 'http://localhost:9002'},
                {'id': 'p3', 'url': 'http://localhost:9003'}
            ]
            
            # Start the mocks in background (using a separate process)
            import subprocess
            base_dir = os.path.dirname(os.path.abspath(__file__))
            runner_script = os.path.join(base_dir, 'mocks/run_mocks.py')
            print("Starting mock servers for dispatcher test...")
            mock_proc = subprocess.Popen([importlib.sys.executable, runner_script], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(2) # wait for mocks to start
            
            try:
                # Trigger dispatcher
                proxy_checker_mod.job_dispatcher(mock_proxies)
                print("✅ Dispatcher executed successfully")
                
                # Wait briefly for Redis writes to settle
                time.sleep(1)
                
                # 6. DB Flusher
                print("\n[6/6] Testing DB Flusher...")
                from app.models.schemas import Base
                from config.database import engine
                Base.metadata.create_all(bind=engine)
                
                db_flusher_mod = importlib.import_module("modules.module-watch.db_flusher")
                db_flusher_mod.flush_history_buffer()
                print("✅ DB Flusher executed successfully")
                
            finally:
                mock_proc.terminate()
                mock_proc.wait()
                
        except Exception as e:
            print(f"❌ Dispatcher/Flusher test FAILED: {e}")

    except Exception as e:

        print(f"❌ Test encountered an error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n=== Tearing Down ===")
        module_watch._shutdown()
        print("Done.")

if __name__ == "__main__":
    run_integration_tests()
