import os
import redis
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Redis Configuration from env
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 5432)) # Default to 5432 as requested
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)

# Create a robust connection pool
# redis_pool = redis.ConnectionPool(
#     host=REDIS_HOST,
#     port=REDIS_PORT,
#     db=REDIS_DB,
#     password=REDIS_PASSWORD,
#     max_connections=100,
#     decode_responses=True
# )

redis_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    max_connections=100,
    decode_responses=True
)

def get_redis_client() -> redis.Redis:
    """
    Returns a configured Redis client instance using the connection pool.
    """
    return redis.Redis(connection_pool=redis_pool)
