import os
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# Singleton Producer Instance
_producer = None

def delivery_report(err, msg):
    """
    Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush().
    """
    if err is not None:
        print(f"[Kafka] Message delivery failed: {err}")
    else:
        print(f"[Kafka] Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def get_kafka_producer() -> Producer:
    """
    Returns a singleton instance of the confluent-kafka Producer.
    """
    global _producer
    if _producer is None:
        dev_status = os.environ.get('dev_status', 'development')
        
        if dev_status == 'production':
            # Cloud (Production) Settings
            bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS_PROD')
            
            # Resolve cert paths to absolute — relative paths break when the process
            # working directory differs from the project root.
            # Anchor to project root (one level up from config/kafka_client.py)
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
            
            def abs_cert(env_var: str) -> str:
                val = os.environ.get(env_var, '')
                if val and not os.path.isabs(val):
                    return os.path.join(project_root, val)
                return val
            
            conf = {
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'proxymaze-producer',
                'acks': 'all',
                'security.protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL_PROD', 'SSL'),
                'ssl.ca.location': abs_cert('KAFKA_SSL_CA_LOCATION_PROD'),
                'ssl.certificate.location': abs_cert('KAFKA_SSL_CERT_LOCATION_PROD'),
                'ssl.key.location': abs_cert('KAFKA_SSL_KEY_LOCATION_PROD'),
            }
            print(f"[Kafka] Using Production (Cloud) Kafka: {bootstrap_servers}")
        else:
            # Local (Development) Settings
            bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS_LOCAL', 'localhost:9092')
            conf = {
                'bootstrap.servers': bootstrap_servers,
                'client.id': 'proxymaze-producer',
                'acks': 'all'
            }
            print(f"[Kafka] Using Development (Local) Kafka: {bootstrap_servers}")

        try:
            _producer = Producer(conf)
            print(f"Kafka Producer initialized successfully.")
        except Exception as e:
            print(f"Failed to initialize Kafka Producer: {e}")
            raise
            
    return _producer

def produce_async(topic: str, key: str, value: str):
    """
    Produces a message asynchronously with the delivery callback attached.
    """
    producer = get_kafka_producer()
    try:
        producer.produce(
            topic=topic,
            key=key.encode('utf-8') if key else None,
            value=value.encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
    except BufferError:
        print(f"[Kafka] Local producer queue is full ({len(producer)} messages awaiting delivery): try again")
    except Exception as e:
        print(f"[Kafka] Exception producing message: {e}")

def flush_producer():
    """
    Ensure all messages in the producer queue are delivered.
    """
    global _producer
    if _producer is not None:
        print("[Kafka] Flushing producer queue...")
        _producer.flush(10)
