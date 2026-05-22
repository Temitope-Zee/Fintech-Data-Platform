import json
import os
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
WAREHOUSE_PATH = os.environ.get('WAREHOUSE_PATH', '/app/warehouse')
BRONZE_PATH = f'{WAREHOUSE_PATH}/bronze/transactions'

os.makedirs(BRONZE_PATH, exist_ok=True)

print(f"Bronze ingestion starting. Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")

# Retry connecting to Kafka up to 10 times
consumer = None
retries = 10

for attempt in range(retries):
    try:
        consumer = KafkaConsumer(
            'clean_transactions',
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='bronze-group'
        )
        print("Connected to Kafka successfully!")
        break
    except NoBrokersAvailable:
        print(f"Kafka not ready. Attempt {attempt + 1}/{retries}. Retrying in 10 seconds...")
        time.sleep(10)

if consumer is None:
    print("Could not connect to Kafka after multiple attempts. Exiting.")
    exit(1)

print(f"Saving data to: {BRONZE_PATH}")
print("Listening to clean_transactions...\n")

batch = []
BATCH_SIZE = 10

for message in consumer:
    record = message.value
    batch.append(record)

    print(f"Received -> ID: {record['transaction_id'][:8]}... | "
          f"Amount: {record['amount']} | Currency: {record['currency']}")

    if len(batch) >= BATCH_SIZE:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f'{BRONZE_PATH}/batch_{timestamp}.json'
        with open(filename, 'w') as f:
            json.dump(batch, f, indent=2)
        print(f"\nWritten {len(batch)} records to Bronze: batch_{timestamp}.json\n")
        batch = []
