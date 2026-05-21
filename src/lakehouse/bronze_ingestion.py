import json
import os
from datetime import datetime
from kafka import KafkaConsumer

# Local warehouse directory — where our Iceberg-style data will live
WAREHOUSE_PATH = os.path.expanduser('~/fintech-data-platform/warehouse')
BRONZE_PATH = f'{WAREHOUSE_PATH}/bronze/transactions'

# Create the directories if they don't exist
os.makedirs(BRONZE_PATH, exist_ok=True)

# Connect to Kafka and read from clean_transactions
consumer = KafkaConsumer(
    'clean_transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='bronze-group'
)

print("Bronze ingestion started. Listening to clean_transactions...")
print(f"Saving data to: {BRONZE_PATH}")
print("Press CTRL+C to stop.\n")

# We'll collect records and write them in batches
batch = []
BATCH_SIZE = 10  # Write to disk every 10 records

for message in consumer:
    record = message.value
    batch.append(record)

    print(f"Received -> ID: {record['transaction_id'][:8]}... | "
          f"Amount: {record['amount']} | Currency: {record['currency']}")

    # When we have enough records, write the batch to disk
    if len(batch) >= BATCH_SIZE:
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f'{BRONZE_PATH}/batch_{timestamp}.json'

        with open(filename, 'w') as f:
            json.dump(batch, f, indent=2)

        print(f"\n✅ Written {len(batch)} records to Bronze layer: batch_{timestamp}.json\n")
        batch = []  # Reset batch
