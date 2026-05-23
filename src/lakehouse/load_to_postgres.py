import json
import os
import psycopg2
from glob import glob

conn = psycopg2.connect(
    host='localhost',
    database='fintech_pipeline',
    user='fintech_user',
    password='fintech123'
)
cursor = conn.cursor()

WAREHOUSE_PATH = os.path.expanduser('~/fintech-data-platform/warehouse')

# Load Bronze data
print("Loading Bronze data into PostgreSQL...")
bronze_files = glob(f'{WAREHOUSE_PATH}/bronze/transactions/*.json')
bronze_count = 0

for file in bronze_files:
    with open(file) as f:
        records = json.load(f)
    for record in records:
        cursor.execute("""
            INSERT INTO bronze_transactions
            (transaction_id, user_id, amount, currency, merchant_category, location, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            record['transaction_id'],
            record['user_id'],
            record['amount'],
            record['currency'],
            record['merchant_category'],
            record['location'],
            record['timestamp']
        ))
        bronze_count += 1

conn.commit()
print(f"Bronze: inserted {bronze_count} records")

# Load Silver data
print("Loading Silver data into PostgreSQL...")
silver_file = f'{WAREHOUSE_PATH}/silver/transactions_clean.json'
silver_count = 0

if os.path.exists(silver_file):
    with open(silver_file) as f:
        records = json.load(f)
    for record in records:
        cursor.execute("""
            INSERT INTO silver_transactions
            (transaction_id, user_id, amount, currency, merchant_category, location, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            record['transaction_id'],
            record['user_id'],
            record['amount'],
            record['currency'],
            record['merchant_category'],
            record['location'],
            record['timestamp']
        ))
        silver_count += 1

conn.commit()
print(f"Silver: inserted {silver_count} records")

# Load Gold data
print("Loading Gold data into PostgreSQL...")
gold_file = f'{WAREHOUSE_PATH}/gold/merchant_metrics.json'
gold_count = 0

if os.path.exists(gold_file):
    with open(gold_file) as f:
        records = json.load(f)
    for record in records:
        cursor.execute("""
            INSERT INTO gold_merchant_metrics
            (merchant_category, total_volume, transaction_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (merchant_category) DO UPDATE
            SET total_volume = EXCLUDED.total_volume,
                transaction_count = EXCLUDED.transaction_count
        """, (
            record['merchant_category'],
            record['total_volume'],
            record['transaction_count']
        ))
        gold_count += 1

conn.commit()
print(f"Gold: inserted {gold_count} records")

cursor.close()
conn.close()
print("\nAll data loaded into PostgreSQL successfully!")
