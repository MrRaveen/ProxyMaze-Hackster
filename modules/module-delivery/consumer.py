import os
import json
import time
import httpx
from confluent_kafka import Consumer, KafkaError
from config.database import get_session
from app.models.schemas import Webhook, WebhookDelivery

def deliver_webhook(url: str, payload: dict, session=None, webhook_id=None) -> bool:
    """
    Attempts to deliver the webhook payload with an exponential backoff retry strategy.
    Retries only on transient errors (5xx) or timeouts.
    Logs every attempt to the database if session is provided.
    """
    max_retries = 3
    base_delay = 1 # seconds
    start_time = time.time()
    
    for attempt in range(max_retries + 1):
        if time.time() - start_time > 55.0:
            print(f"[Delivery] ❌ 60-second delivery bound reached for {url}.")
            return False
            
        success = False
        try:
            # Time constraint: 10s timeout per attempt
            response = httpx.post(url, json=payload, timeout=10.0)
            
            if 200 <= response.status_code < 300:
                print(f"[Delivery] ✅ Successfully delivered to {url}")
                success = True
            elif response.status_code in {500, 502, 503, 504}:
                print(f"[Delivery] ⚠️ Transient error {response.status_code} for {url}.")
                success = False
            else:
                print(f"[Delivery] ❌ Client error {response.status_code} for {url}. Giving up.")
                success = False
                
        except (httpx.TimeoutException, httpx.RequestError) as e:
            print(f"[Delivery] ⚠️ Request error/timeout delivering to {url}: {e}")
            success = False
            
        # Log attempt to DB
        if session and webhook_id:
            try:
                from app.models.schemas import WebhookDelivery
                delivery_log = WebhookDelivery(
                    webhook_id=webhook_id,
                    success=success
                )
                session.add(delivery_log)
                session.commit()
            except Exception as log_err:
                print(f"[Delivery Engine] Failed to log attempt to DB: {log_err}")

        if success:
            return True
            
        # If we are here, it was a failure. Check if we should retry.
        if 'response' in locals() and response.status_code not in {500, 502, 503, 504}:
            return False

        if attempt < max_retries:
            delay = base_delay * (2 ** attempt)
            print(f"[Delivery] Retrying {url} in {delay}s (Attempt {attempt + 1}/{max_retries})...")
            time.sleep(delay)
            
    print(f"[Delivery] ❌ Failed to deliver to {url} after {max_retries} retries.")
    return False

def start_consumer():
    """
    Starts the Kafka Consumer loop.
    Intended to be run in a separate process.
    """
    dev_status = os.environ.get('dev_status', 'development')
    group_id = os.environ.get('KAFKA_GROUP_ID', 'proxymaze-delivery-engine')
    
    if dev_status == 'production':
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS_PROD')
        
        # Resolve cert paths to absolute — relative paths break in multiprocessing subprocesses
        # because the child process may have a different working directory.
        # We anchor to the project root (two levels up from this file: modules/module-delivery/consumer.py)
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        
        def abs_cert(env_var: str) -> str:
            val = os.environ.get(env_var, '')
            if val and not os.path.isabs(val):
                return os.path.join(project_root, val)
            return val
        
        ca_location   = abs_cert('KAFKA_SSL_CA_LOCATION_PROD')
        cert_location = abs_cert('KAFKA_SSL_CERT_LOCATION_PROD')
        key_location  = abs_cert('KAFKA_SSL_KEY_LOCATION_PROD')
        
        # Validate files exist before handing to librdkafka
        for label, path in [('CA', ca_location), ('Cert', cert_location), ('Key', key_location)]:
            if not os.path.isfile(path):
                print(f"[Delivery Engine] ❌ SSL {label} file not found: {path}")
                return
        
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'security.protocol': os.environ.get('KAFKA_SECURITY_PROTOCOL_PROD', 'SSL'),
            'ssl.ca.location': ca_location,
            'ssl.certificate.location': cert_location,
            'ssl.key.location': key_location,
        }
        print(f"[Delivery Engine] Connecting to Production Kafka: {bootstrap_servers}")
        print(f"[Delivery Engine] Using SSL CA: {ca_location}")
    else:
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS_LOCAL', 'localhost:9092')
        conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        print(f"[Delivery Engine] Connecting to Development Kafka: {bootstrap_servers}")
    
    consumer = Consumer(conf)
    topics = ['alert.fired', 'alert.resolved']
    
    try:
        consumer.subscribe(topics)
        print(f"[Delivery Engine] Started listening to Kafka topics: {topics}")
        
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                err_code = msg.error().code()
                if err_code == KafkaError._PARTITION_EOF:
                    continue
                elif err_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    time.sleep(1)
                    continue
                else:
                    print(f"[Delivery Engine] ❌ Kafka error: {msg.error()}")
                    break
                    
            topic = msg.topic()
            payload = json.loads(msg.value().decode('utf-8'))
            print(f"\n[Delivery Engine] Received {topic} event for Alert ID {payload.get('alert_id')}")
            
            try:
                with get_session() as session:
                    webhooks = session.query(Webhook).all()
                    
                    for webhook in webhooks:
                        # Skip explicitly blocked integrations
                        if webhook.integration_type in ('slack', 'discord'):
                            print(f"[Delivery Engine] 🚫 Skipping blocked integration type: {webhook.integration_type} for {webhook.url}")
                            continue
                            
                        if topic in webhook.events:
                            deliver_webhook(
                                url=webhook.url, 
                                payload=payload, 
                                session=session, 
                                webhook_id=webhook.webhook_id
                            )
                    session.commit()
            except Exception as e:
                print(f"[Delivery Engine] ❌ Error processing webhooks: {e}")
                continue
                
            consumer.commit(asynchronous=False)
            print(f"[Delivery Engine] Offset committed for {topic}.")
            
    except KeyboardInterrupt:
        print("[Delivery Engine] Shutdown requested.")
    finally:
        consumer.close()
        print("[Delivery Engine] Consumer closed.")

if __name__ == "__main__":
    start_consumer()
