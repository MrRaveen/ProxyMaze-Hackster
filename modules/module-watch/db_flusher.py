import json
from datetime import datetime, timezone
from sqlalchemy import insert
from config.redis_client import get_redis_client
from config.database import get_session
from app.models.schemas import CheckResult

def flush_history_buffer():
    """
    Background task to drain the Redis history buffer and bulk insert
    into PostgreSQL to prevent connection exhaustion.
    """
    client = get_redis_client()
    
    # We use a pipeline to ensure atomic extraction and trimming
    pipe = client.pipeline()
    pipe.lrange("proxymaze:history_buffer", 0, -1)
    pipe.ltrim("proxymaze:history_buffer", 1, 0) # Clears the list
    results = pipe.execute()
    
    # results[0] contains the items from lrange
    raw_records = results[0]
    
    if not raw_records:
        return
        
    print(f"[DB Flusher] Draining {len(raw_records)} records from Redis to PostgreSQL...")
    
    parsed_records = []
    for raw in raw_records:
        try:
            record = json.loads(raw)
            # Ensure the checked_at string is converted back to a datetime object
            checked_at = datetime.strptime(record['checked_at'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            
            parsed_records.append({
                "proxy_id": record["proxy_id"],
                "status": record["status"],
                "checked_at": checked_at
            })
        except Exception as e:
            print(f"[DB Flusher] Error parsing record: {e}")
            
    if not parsed_records:
        return
        
    # Bulk insert into PostgreSQL within a single transaction
    try:
        with get_session() as session:
            session.execute(insert(CheckResult).values(parsed_records))
            # The get_session context manager handles the commit()
        print(f"[DB Flusher] Successfully flushed {len(parsed_records)} records to DB.")
    except Exception as e:
        print(f"[DB Flusher] Critical error flushing to DB: {e}")
        # In a real production system, we might want to push these back to Redis
        # or a dead-letter queue if the DB goes down entirely.
