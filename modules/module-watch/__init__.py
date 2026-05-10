import atexit
import multiprocessing
import importlib
from flask import Flask
from config.redis_client import get_redis_client
from .scheduler import init_scheduler, shutdown_scheduler
from .pubsub_listener import start_pubsub_listener, stop_pubsub_listener
from .lua_scripts import register_scripts
from config.kafka_client import flush_producer

_consumer_process = None

def init_app(app: Flask):
    """
    Initializes the watch subsystem within the Flask application context.
    """
    global _consumer_process
    import os
    
    # In development mode, Flask runs the app twice (once for the reloader, once for the worker).
    # We only want to start background processes in the main worker process.
    if app.debug and os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
        # Reloader process: don't start background tasks
        return

    with app.app_context():
        print("Initializing Watch subsystem...")
        
        # 1. Ensure Redis connection is viable
        client = get_redis_client()
        client.ping()
        print("Redis connection pool initialized and verified.")
        
        # 2. Register Lua scripts
        register_scripts()
        
        # 3. Start the background scheduler
        init_scheduler()
        
        # 4. Start the Pub/Sub listener daemon thread
        start_pubsub_listener()

        # 5. Start Kafka Delivery Consumer (Process)
        try:
            consumer_mod = importlib.import_module("modules.module-delivery.consumer")
            _consumer_process = multiprocessing.Process(target=consumer_mod.start_consumer, daemon=True)
            _consumer_process.start()
            print("Kafka Delivery Consumer process started.")
        except Exception as e:
            print(f"Failed to start Kafka Delivery Consumer: {e}")
        
        # Register shutdown handlers to ensure clean exit
        atexit.register(_shutdown)

def _shutdown():
    """
    Gracefully shuts down all background processes associated with the module.
    """
    global _consumer_process
    print("Shutting down Watch subsystem...")
    stop_pubsub_listener()
    shutdown_scheduler()
    flush_producer()
    
    if _consumer_process and _consumer_process.is_alive():
        _consumer_process.terminate()
        _consumer_process.join()
        print("Kafka Delivery Consumer process terminated.")
        
    print("Watch subsystem shutdown complete.")
