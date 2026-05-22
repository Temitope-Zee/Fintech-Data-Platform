import json
import os
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

consumer = KafkaConsumer(
    'raw_transactions',
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='validator-group'
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

VALID_CURRENCIES = ['USD', 'EUR', 'GBP', 'NGN', 'JPY']

def validate(record):
    if record.get('amount') is None:
        return False
    if record['amount'] <= 0:
        return False
    if record.get('currency') not in VALID_CURRENCIES:
        return False
    if not record.get('transaction_id'):
        return False
    return True

print(f"Validator started. Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
print("Listening to raw_transactions...")
print("Press CTRL+C to stop.\n")

for message in consumer:
    record = message.value
    if validate(record):
        producer.send('clean_transactions', value=record)
        print(f"VALID   -> ID: {record['transaction_id'][:8]}... | Amount: {record['amount']} | Currency: {record['currency']}")
    else:
        producer.send('dead_letter_queue', value=record)
        print(f"INVALID -> ID: {record['transaction_id'][:8]}... | Amount: {record['amount']} | Currency: {record['currency']}")
