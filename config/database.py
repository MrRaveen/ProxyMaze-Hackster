import os
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment Selection
dev_status = os.environ.get('dev_status', 'development')

if dev_status == 'production':
    # Cloud (Production) Settings
    DATABASE_URL = os.environ.get('DATABASE_URL_PROD')
    if not DATABASE_URL:
        DB_USER = os.environ.get('DB_USER_PROD')
        DB_PASSWORD = os.environ.get('DB_PASSWORD_PROD')
        DB_HOST = os.environ.get('DB_HOST_PROD')
        DB_PORT = os.environ.get('DB_PORT_PROD', '5432')
        DB_NAME = os.environ.get('DB_NAME_PROD')
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    print(f"[DB] Using Production (Cloud) database: {os.environ.get('DB_HOST_PROD')}")
else:
    # Local (Development) Settings
    DB_USER = os.environ.get('DB_USER_LOCAL', 'admin')
    DB_PASSWORD = os.environ.get('DB_PASSWORD_LOCAL', 'admin')
    DB_HOST = os.environ.get('DB_HOST_LOCAL', 'localhost')
    DB_PORT = os.environ.get('DB_PORT_LOCAL', '5432')
    DB_NAME = os.environ.get('DB_NAME_LOCAL', 'proxymaze_db')
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print(f"[DB] Using Development (Local) database: {DB_HOST}")

# Initialize engine
engine = create_engine(
    DATABASE_URL,
    future=True,
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
