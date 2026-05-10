from config.database import get_session
from app.models.schemas import Config

with get_session() as session:
    config = session.query(Config).first()
    if config:
        print(f"ID: {config.id}")
        print(f"Interval: {config.check_interval_seconds}")
        print(f"Timeout: {config.request_timeout_ms}")
    else:
        print("No config found")
