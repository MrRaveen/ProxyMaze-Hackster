import requests
import concurrent.futures
from typing import List, Dict

from .scheduler import config_state
from .lua_scripts import get_script

# Global executor for running proxy checks concurrently
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

# Keys for the Redis Lua script
PROXY_STATES_KEY = "proxymaze:state:proxies"
DOWN_COUNT_KEY = "proxymaze:state:down_count"
THRESHOLD = 0.20 # 20%

def proxy_worker(proxy: Dict, total_pool_size: int):
    """
    Worker function to check a single proxy.
    Enforces hackathon classification rules and atomically updates the failure rate.
    """
    proxy_id = proxy['id']
    proxy_url = proxy['url']
    
    # Get timeout (assuming the config might have it in ms or seconds)
    timeout_ms = config_state.get('request_timeout_ms')
    if timeout_ms is not None:
        timeout_sec = timeout_ms / 1000.0
    else:
        timeout_sec = config_state.get('request_timeout', 3)
    
    status = 'down'
    
    try:
        # Construct the probe URL. If the URL already has query params, 
        # we insert the endpoint before them.
        if '?' in proxy_url:
            base_url, query = proxy_url.split('?', 1)
            probe_url = f"{base_url.rstrip('/')}/proxy/{proxy_id}?{query}"
        else:
            probe_url = f"{proxy_url.rstrip('/')}/proxy/{proxy_id}"
            
        response = requests.get(probe_url, timeout=timeout_sec)
        if 200 <= response.status_code < 300:
            status = 'up'
        elif 500 <= response.status_code < 600:
            status = 'down'
    except (requests.Timeout, requests.ConnectionError, requests.RequestException):
        status = 'down'
    except Exception as e:
        status = 'down'
        print(f"[{proxy_id}] Unexpected error during request: {e}")

    # Atomically update state and check threshold
    try:
        script = get_script('increment_and_check')
        # KEYS: [proxy_states_key, down_count_key]
        # ARGV: [proxy_id, new_state, total_pool_size, threshold]
        result = script(
            keys=[PROXY_STATES_KEY, DOWN_COUNT_KEY],
            args=[proxy_id, status, total_pool_size, THRESHOLD]
        )
        
        if result in (1, 2):
            # We need to fetch current failure rate and failed proxies
            from config.redis_client import get_redis_client
            from config.kafka_client import produce_async
            import uuid
            import json
            from datetime import datetime, timezone
            
            client = get_redis_client()
            down_count = int(client.get(DOWN_COUNT_KEY) or 0)
            failure_rate = down_count / total_pool_size
            
            all_states = client.hgetall(PROXY_STATES_KEY)
            failed_proxy_ids = [k for k, v in all_states.items() if v == 'down']
            now = datetime.now(timezone.utc)
            fired_at_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            if result == 1:
                # FIRED
                print(f"CRITICAL: Failure rate threshold ({THRESHOLD*100}%) crossed upwards! (Triggered by proxy {proxy_id} going {status})")
                alert_id = str(uuid.uuid4())
                client.set("proxymaze:active_alert_id", alert_id)
                topic = "alert.fired"
                
                # Persist Alert to DB
                try:
                    from config.database import get_session
                    from app.models.schemas import Alert
                    with get_session() as session:
                        alert = Alert(
                            alert_id=alert_id,
                            status='active',
                            failure_rate=failure_rate,
                            fired_at=now,
                            total_proxies=total_pool_size,
                            failed_proxies=down_count,
                            failed_proxy_ids=failed_proxy_ids,
                            threshold=THRESHOLD,
                            message=f"Threshold {THRESHOLD*100}% breached. {down_count}/{total_pool_size} proxies down."
                        )
                        session.add(alert)
                        # Let context manager handle commit
                except Exception as db_err:
                    print(f"Error persisting Alert to DB: {db_err}")
                    import traceback; traceback.print_exc()
                    
            else:
                # RESOLVED
                print(f"RESOLVED: Failure rate dropped below threshold ({THRESHOLD*100}%). (Triggered by proxy {proxy_id} going {status})")
                alert_id = client.get("proxymaze:active_alert_id") or str(uuid.uuid4())
                client.delete("proxymaze:active_alert_id")
                topic = "alert.resolved"
                
                # Update Alert in DB
                try:
                    from config.database import get_session
                    from app.models.schemas import Alert
                    with get_session() as session:
                        alert = session.get(Alert, alert_id)
                        if alert:
                            alert.status = 'resolved'
                            alert.resolved_at = now
                            # Let context manager handle commit
                except Exception as db_err:
                    print(f"Error resolving Alert in DB: {db_err}")
                    import traceback; traceback.print_exc()
                
            # Kafka payload — must match the GET /alerts record exactly
            payload = {
                "alert_id": alert_id,
                "failure_rate": failure_rate,
                "total_proxies": total_pool_size,
                "failed_proxies": down_count,
                "failed_proxy_ids": failed_proxy_ids,
                "threshold": THRESHOLD,
                "fired_at": fired_at_str
            }
            
            # Asynchronous Produce Pattern
            produce_async(
                topic=topic,
                key=alert_id,
                value=json.dumps(payload)
            )
            
    except Exception as e:
        print(f"[{proxy_id}] Error executing Redis Lua script: {e}")
        
    # High-Throughput State Persistence Buffer
    try:
        # We need these imports inside the file
        from config.redis_client import get_redis_client
        import json
        from datetime import datetime, timezone
        
        checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        client = get_redis_client()
        pipe = client.pipeline()
        
        # 1. Update the proxy's live state
        pipe.hset(f"proxymaze:live:{proxy_id}", mapping={"status": status, "checked_at": checked_at})
        
        # 2. Append history record
        history_record = json.dumps({
            "proxy_id": proxy_id,
            "status": status,
            "checked_at": checked_at
        })
        pipe.rpush("proxymaze:history_buffer", history_record)
        
        # Execute pipeline
        pipe.execute()
    except Exception as e:
        print(f"[{proxy_id}] Error buffering state to Redis: {e}")
        
    return proxy_id, status

def job_dispatcher(active_proxies: List[Dict]):
    """
    APScheduler triggered function.
    Iterates through the proxy pool and submits each as a concurrent task to the ThreadPoolExecutor.
    """
    total_pool_size = len(active_proxies)
    if total_pool_size == 0:
        return
        
    print(f"Dispatching checks for {total_pool_size} proxies...")
    
    futures = {}
    for proxy in active_proxies:
        future = _executor.submit(proxy_worker, proxy, total_pool_size)
        futures[future] = proxy['id']
        
    # Collect futures and handle exceptions
    for future in concurrent.futures.as_completed(futures):
        proxy_id = futures[future]
        try:
            # Result is (proxy_id, status)
            pid, status = future.result()
            print(f"[{pid}] Check complete: {status}")
        except Exception as e:
            # Catch unhandled thread exceptions so they don't crash the main loop
            print(f"[{proxy_id}] Unhandled thread exception: {e}")
