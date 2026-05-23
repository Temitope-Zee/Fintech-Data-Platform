import json
import os
import boto3
from botocore.client import Config
from collections import defaultdict
from datetime import datetime

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'minioadmin123')
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'fintech-lakehouse')

# Connect to MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

print(f"Connected to MinIO at {MINIO_ENDPOINT}")
print(f"Processing bucket: {MINIO_BUCKET}\n")

# --- BRONZE → SILVER ---
print("Reading Bronze layer from MinIO...")

# List all bronze batch files
response = s3_client.list_objects_v2(
    Bucket=MINIO_BUCKET,
    Prefix='bronze/transactions/'
)

all_records = []

if 'Contents' in response:
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.json'):
            data = s3_client.get_object(Bucket=MINIO_BUCKET, Key=key)
            records = json.loads(data['Body'].read().decode('utf-8'))
            all_records.extend(records)

print(f"Total records read from Bronze: {len(all_records)}")

# Deduplicate by transaction_id
seen_ids = set()
silver_records = []
for record in all_records:
    if record['transaction_id'] not in seen_ids:
        seen_ids.add(record['transaction_id'])
        silver_records.append(record)

print(f"After deduplication (Silver): {len(silver_records)} records")

# Save Silver to MinIO
silver_key = 'silver/transactions_clean.json'
s3_client.put_object(
    Bucket=MINIO_BUCKET,
    Key=silver_key,
    Body=json.dumps(silver_records, indent=2),
    ContentType='application/json'
)

print(f"Silver saved to MinIO: minio://{MINIO_BUCKET}/{silver_key}")

# --- SILVER → GOLD ---
print("\nCalculating Gold layer aggregations...")

metrics = defaultdict(lambda: {'total_volume': 0.0, 'transaction_count': 0})

for record in silver_records:
    category = record['merchant_category']
    metrics[category]['total_volume'] += record['amount']
    metrics[category]['transaction_count'] += 1

gold_records = [
    {
        'merchant_category': cat,
        'total_volume': round(data['total_volume'], 2),
        'transaction_count': data['transaction_count'],
        'calculated_at': datetime.utcnow().isoformat()
    }
    for cat, data in metrics.items()
]

# Save Gold to MinIO
gold_key = 'gold/merchant_metrics.json'
s3_client.put_object(
    Bucket=MINIO_BUCKET,
    Key=gold_key,
    Body=json.dumps(gold_records, indent=2),
    ContentType='application/json'
)

print(f"Gold saved to MinIO: minio://{MINIO_BUCKET}/{gold_key}")

print("\nGold Layer - Merchant Metrics:")
print("-" * 50)
for row in sorted(gold_records, key=lambda x: x['total_volume'], reverse=True):
    print(f"  {row['merchant_category']:<15} | "
          f"Volume: {row['total_volume']:>10.2f} | "
          f"Count: {row['transaction_count']}")

print("\nMinIO Silver and Gold ETL completed successfully!")
