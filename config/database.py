import os
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL Configuration from env
DB_USER = os.environ.get('DB_USER', 'proxymaze_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'proxymaze_pass')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = os.environ.get('DB_PORT', '6379') # Default to 6379 as requested
DB_NAME = os.environ.get('DB_NAME', 'proxymaze_db')

# Construct PostgreSQL URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Initialize engine
engine = create_engine(
    DATABASE_URL,
    future=True,
    # Standard pool settings for high-throughput
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

# Initialize SessionLocal
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True
)

@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions to ensure proper cleanup.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
