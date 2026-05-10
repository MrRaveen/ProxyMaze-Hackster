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
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'proxymaze-producer',
            # Add other configurations for acks, retries, etc. if needed
            'acks': 'all'
        }
        try:
            _producer = Producer(conf)
            print(f"Kafka Producer initialized for {bootstrap_servers}")
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
        # Produce message
        producer.produce(
            topic=topic,
            key=key.encode('utf-8') if key else None,
            value=value.encode('utf-8'),
            callback=delivery_report
        )
        # Serve delivery callback queue.
        # This is required to trigger the delivery_report callback.
        producer.poll(0)
    except BufferError:
        print(f"[Kafka] Local producer queue is full ({len(producer)} messages awaiting delivery): try again")
    except Exception as e:
        print(f"[Kafka] Exception producing message: {e}")

def flush_producer():
    """
    Ensure all messages in the producer queue are delivered.
    Call this on shutdown.
    """
    global _producer
    if _producer is not None:
        print("[Kafka] Flushing producer queue...")
        _producer.flush(10) # wait up to 10 seconds
