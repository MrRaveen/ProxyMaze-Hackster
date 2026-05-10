from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

# We will initialize the scheduler here so it can be imported across the module
scheduler = BackgroundScheduler()

# Configuration state (will be updated dynamically via Pub/Sub)
# Default values
config_state = {
    'check_interval': 60, # Seconds
    'request_timeout': 3  # Seconds
}

def dispatch_all_proxies():
    from config.database import get_session
    from app.models.schemas import Proxy
    from sqlalchemy import select
    from .proxy_checker import job_dispatcher
    
    with get_session() as session:
        statement = select(Proxy)
        proxies = session.execute(statement).scalars().all()
        active_proxies = [{'id': p.id, 'url': p.url} for p in proxies]
        job_dispatcher(active_proxies)


def init_scheduler():
    """
    Initializes and starts the APScheduler with a ThreadPoolExecutor.
    """
    # Configure executor for high-throughput background polling
    executors = {
        'default': ThreadPoolExecutor(20)
    }
    
    # Configure job defaults
    job_defaults = {
        'coalesce': True,    # Roll missed executions into a single trigger
        'max_instances': 1   # STRICT: Never allow overlapping check intervals
    }
    
    scheduler.configure(executors=executors, job_defaults=job_defaults)
    
    # Import locally to avoid circular imports if any
    from .db_flusher import flush_history_buffer
    
    # Register the high-throughput DB flusher
    scheduler.add_job(
        flush_history_buffer, 
        'interval', 
        seconds=30, 
        id='db_flusher',
        max_instances=1,
        replace_existing=True
    )
    
    # Bootstrap config and active alert state
    from config.database import get_session
    from app.models.schemas import Config, Alert
    from sqlalchemy import select
    from config.redis_client import get_redis_client
    
    with get_session() as session:
        db_config = session.execute(select(Config)).scalars().first()
        if db_config:
            config_state['check_interval'] = db_config.check_interval_seconds
            config_state['request_timeout'] = db_config.request_timeout_ms
            print(f"Bootstrapped config from DB: {config_state}")
            
        active_alert = session.execute(select(Alert).where(Alert.status == 'active')).scalars().first()
        if active_alert:
            client = get_redis_client()
            client.set("proxymaze:active_alert_id", active_alert.alert_id)
            print(f"Restored active alert ID to Redis: {active_alert.alert_id}")

    # Register the continuous execution proxy probe loop
    scheduler.add_job(
        dispatch_all_proxies,
        'interval',
        seconds=config_state['check_interval'],
        id='proxy_checker',
        max_instances=1,
        replace_existing=True
    )
    
    scheduler.start()
    print("BackgroundScheduler started with ThreadPoolExecutor.")

def shutdown_scheduler():
    """
    Gracefully shuts down the scheduler.
    """
    if scheduler.running:
        scheduler.shutdown()
        print("BackgroundScheduler shut down.")

def update_config_state(new_config: dict):
    """
    Updates the local memory config state.
    This will be called by the Pub/Sub listener.
    """
    global config_state
    for key, value in new_config.items():
        if key in config_state:
            config_state[key] = value
            print(f"[Scheduler] Updated config: {key} = {value}")
            
    # Reschedule running jobs based on the new interval.
    if 'check_interval' in new_config and scheduler.get_job('proxy_checker'):
        scheduler.reschedule_job('proxy_checker', trigger='interval', seconds=new_config['check_interval'])
        print(f"[Scheduler] Rescheduled proxy_checker interval to {new_config['check_interval']}s")
