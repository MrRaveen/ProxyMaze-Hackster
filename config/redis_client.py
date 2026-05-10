import os
import redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment Selection
dev_status = os.environ.get('dev_status', 'development')

if dev_status == 'production':
    # Cloud (Production) Settings
    REDIS_HOST = os.environ.get('REDIS_HOST_PROD')
    REDIS_PORT = int(os.environ.get('REDIS_PORT_PROD', 6379))
    REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD_PROD')
    REDIS_SSL = True
    print(f"[Redis] Using Production (Cloud) Redis: {REDIS_HOST}")
else:
    # Local (Development) Settings
    REDIS_HOST = os.environ.get('REDIS_HOST_LOCAL', 'localhost')
    REDIS_PORT = int(os.environ.get('REDIS_PORT_LOCAL', 6379))
    REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD_LOCAL')
    REDIS_SSL = False
    print(f"[Redis] Using Development (Local) Redis: {REDIS_HOST}")

# Connection Configuration
redis_kwargs = {
    'host': REDIS_HOST,
    'port': REDIS_PORT,
    'db': 0,
    'password': REDIS_PASSWORD,
    'decode_responses': True,
    'socket_timeout': 5,
    'retry_on_timeout': True
}

if REDIS_SSL:
    redis_kwargs['ssl'] = True
    redis_kwargs['ssl_cert_reqs'] = None  # Common for cloud providers like Upstash

# Create a robust client (it manages its own pool internally by default)
_redis_client = redis.Redis(**redis_kwargs)

def get_redis_client() -> redis.Redis:
    """
    Returns a configured Redis client instance.
    """
    return _redis_client
