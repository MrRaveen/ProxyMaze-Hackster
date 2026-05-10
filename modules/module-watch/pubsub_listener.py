import json
import threading
import time
from config.redis_client import get_redis_client
from .scheduler import update_config_state

# The Redis channel we will listen to for configuration updates
CONFIG_CHANNEL = "proxymaze:config:updates"

# Global reference to keep track of the listener thread and pubsub object
_listener_thread = None
_pubsub = None
_stop_event = threading.Event()

def _listen_for_updates():
    """
    Background worker loop for listening to Pub/Sub messages.
    """
    redis_client = get_redis_client()
    global _pubsub
    
    try:
        _pubsub = redis_client.pubsub()
        _pubsub.subscribe(CONFIG_CHANNEL)
        print(f"Subscribed to Redis channel: {CONFIG_CHANNEL}")
        
        while not _stop_event.is_set():
            # get_message(ignore_subscribe_messages=True, timeout=1.0)
            message = _pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                try:
                    data = json.loads(message['data'])
                    print(f"Received config update via Pub/Sub: {data}")
                    update_config_state(data)
                except json.JSONDecodeError:
                    print(f"Invalid JSON received on {CONFIG_CHANNEL}: {message['data']}")
                except Exception as e:
                    print(f"Error processing pub/sub message: {e}")
                    
    except Exception as e:
        print(f"Redis Pub/Sub listener error: {e}")
    finally:
        if _pubsub:
            _pubsub.close()
            print(f"Unsubscribed from {CONFIG_CHANNEL}")

def start_pubsub_listener():
    """
    Starts the Redis Pub/Sub listener in a background daemon thread.
    """
    global _listener_thread
    _stop_event.clear()
    
    _listener_thread = threading.Thread(target=_listen_for_updates, daemon=True, name="RedisPubSubListener")
    _listener_thread.start()
    print("Redis Pub/Sub listener thread started.")

def stop_pubsub_listener():
    """
    Signals the Pub/Sub listener thread to stop.
    """
    print("Stopping Redis Pub/Sub listener...")
    _stop_event.set()
    if _listener_thread and _listener_thread.is_alive():
        _listener_thread.join(timeout=2.0)
        print("Redis Pub/Sub listener stopped.")
