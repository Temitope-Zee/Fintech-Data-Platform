import json
import random
from kafka import KafkaConsumer, KafkaProducer

# Connect to Kafka to READ from raw_transactions
consumer = KafkaConsumer(
    'raw_transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='validator-group'
)

# Connect to Kafka to WRITE validated results
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

VALID_CURRENCIES = ['USD', 'EUR', 'GBP', 'NGN', 'JPY']

def validate(record):
    """
    Returns True if the record passes all quality checks.
    A valid transaction must have:
    - A positive amount
    - A recognised currency
    - A transaction ID
    """
    if record.get('amount') is None:
        return False
    if record['amount'] <= 0:
        return False
    if record.get('currency') not in VALID_CURRENCIES:
        return False
    if not record.get('transaction_id'):
        return False
    return True

print("Validator started. Listening to raw_transactions...")
print("Press CTRL+C to stop.\n")

for message in consumer:
    record = message.value

    if validate(record):
        producer.send('clean_transactions', value=record)
        print(f"VALID   -> ID: {record['transaction_id'][:8]}... | "
              f"Amount: {record['amount']} | Currency: {record['currency']}")
    else:
        producer.send('dead_letter_queue', value=record)
        print(f"INVALID -> ID: {record['transaction_id'][:8]}... | "
              f"Amount: {record['amount']} | Currency: {record['currency']}")
