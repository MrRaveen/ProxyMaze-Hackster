import json
import traceback
from datetime import datetime, timezone
from config.redis_client import get_redis_client
from config.database import get_session
from app.models.schemas import CheckResult, Proxy


def flush_history_buffer():
    """
    Background task to drain the Redis history buffer and bulk insert
    into PostgreSQL to prevent connection exhaustion.
    """
    client = get_redis_client()

    # We use a pipeline to ensure atomic extraction and trimming
    pipe = client.pipeline()
    pipe.lrange("proxymaze:history_buffer", 0, -1)
    pipe.ltrim("proxymaze:history_buffer", 1, 0)  # Clears the list
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
            checked_at = datetime.strptime(
                record['checked_at'], "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)

            parsed_records.append({
                "proxy_id": record["proxy_id"],
                "status": record["status"],
                "checked_at": checked_at
            })
        except Exception as e:
            print(f"[DB Flusher] Error parsing record: {e}")

    if not parsed_records:
        return

    # Persist to PostgreSQL within a single transaction using ORM objects
    try:
        with get_session() as session:
            # Insert CheckResult rows using ORM adds (thread-safe)
            for record in parsed_records:
                check = CheckResult(
                    proxy_id=record["proxy_id"],
                    status=record["status"],
                    checked_at=record["checked_at"]
                )
                session.add(check)

            # Group by proxy_id to find the latest state and calculate batch stats
            proxy_updates = {}
            for record in parsed_records:
                pid = record["proxy_id"]
                if pid not in proxy_updates:
                    proxy_updates[pid] = {
                        "status": record["status"],
                        "last_checked_at": record["checked_at"],
                        "batch_total": 0,
                        "batch_success": 0,
                        "batch_failures": 0
                    }
                else:
                    if record["checked_at"] > proxy_updates[pid]["last_checked_at"]:
                        proxy_updates[pid]["status"] = record["status"]
                        proxy_updates[pid]["last_checked_at"] = record["checked_at"]

                proxy_updates[pid]["batch_total"] += 1
                if record["status"] == "up":
                    proxy_updates[pid]["batch_success"] += 1
                else:
                    proxy_updates[pid]["batch_failures"] += 1

            # Apply updates to the Proxy table
            for pid, data in proxy_updates.items():
                proxy = session.get(Proxy, pid)
                if proxy:
                    proxy.status = data["status"]
                    proxy.last_checked_at = data["last_checked_at"]

                    if data["status"] == "down":
                        proxy.consecutive_failures += data["batch_failures"]
                    else:
                        proxy.consecutive_failures = 0

                    proxy.total_checks += data["batch_total"]
                    proxy.successful_checks += data["batch_success"]

            # get_session context manager handles commit()

        print(f"[DB Flusher] ✅ Flushed {len(parsed_records)} records and updated {len(proxy_updates)} proxies.")
    except Exception as e:
        print(f"[DB Flusher] ❌ Critical error flushing to DB: {e}")
        traceback.print_exc()
