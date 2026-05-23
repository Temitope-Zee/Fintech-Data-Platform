import json
import os
import time
import boto3
from botocore.client import Config
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'fintech-lakehouse')

WAREHOUSE_PATH = os.environ.get('WAREHOUSE_PATH', '/app/warehouse')
BRONZE_PATH = f'{WAREHOUSE_PATH}/bronze/transactions'
os.makedirs(BRONZE_PATH, exist_ok=True)

# Connect to MinIO using boto3
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Create bucket if it doesn't exist
try:
    s3_client.head_bucket(Bucket=MINIO_BUCKET)
    print(f"Bucket {MINIO_BUCKET} already exists")
except:
    s3_client.create_bucket(Bucket=MINIO_BUCKET)
    print(f"Created bucket: {MINIO_BUCKET}")

print(f"Bronze ingestion starting...")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"MinIO: {MINIO_ENDPOINT} | Bucket: {MINIO_BUCKET}")

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

        # Upload to MinIO
        s3_key = f'bronze/transactions/batch_{timestamp}.json'
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=s3_key,
            Body=json.dumps(batch, indent=2),
            ContentType='application/json'
        )

        print(f"\nWritten {len(batch)} records to:")
        print(f"  Local: {local_file}")
        print(f"  MinIO: minio://{MINIO_BUCKET}/{s3_key}\n")
        batch = []
