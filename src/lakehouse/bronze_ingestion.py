import json
import os
import time
import boto3
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
AWS_REGION = os.environ.get('AWS_REGION', 'eu-north-1')
S3_BUCKET = os.environ.get('S3_BUCKET', 'fintech-lakehouse-zainab-2026')

# Also keep local backup
WAREHOUSE_PATH = os.environ.get('WAREHOUSE_PATH', '/app/warehouse')
BRONZE_PATH = f'{WAREHOUSE_PATH}/bronze/transactions'
os.makedirs(BRONZE_PATH, exist_ok=True)

# Connect to S3
s3_client = boto3.client('s3', region_name=AWS_REGION)

print(f"Bronze ingestion starting...")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"S3 Bucket: {S3_BUCKET}")

# Retry connecting to Kafka
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
    print("Could not connect to Kafka. Exiting.")
    exit(1)

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

        # Save locally
        local_file = f'{BRONZE_PATH}/batch_{timestamp}.json'
        with open(local_file, 'w') as f:
            json.dump(batch, f, indent=2)

        # Upload to S3
        s3_key = f'bronze/transactions/batch_{timestamp}.json'
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(batch, indent=2),
            ContentType='application/json'
        )

        print(f"\nWritten {len(batch)} records to:")
        print(f"  Local: {local_file}")
        print(f"  S3: s3://{S3_BUCKET}/{s3_key}\n")
        batch = []
