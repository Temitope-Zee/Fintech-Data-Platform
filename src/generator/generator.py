
import json
import time
import random
import os
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CURRENCIES = ['USD', 'EUR', 'GBP', 'NGN', 'JPY']
CATEGORIES = ['groceries', 'electronics', 'travel', 'dining', 'utilities']

def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "amount": round(random.uniform(-50, 10000), 2),
        "currency": random.choice(CURRENCIES + ['INVALID']),
        "merchant_category": random.choice(CATEGORIES),
        "location": fake.city(),
        "timestamp": datetime.utcnow().isoformat()
    }

print(f"Generator started. Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
print("Press CTRL+C to stop.\n")

while True:
    txn = generate_transaction()
    producer.send('raw_transactions', value=txn)
    print(f"Sent -> ID: {txn['transaction_id'][:8]}... | Amount: {txn['amount']} | Currency: {txn['currency']}")
    time.sleep(0.5)
