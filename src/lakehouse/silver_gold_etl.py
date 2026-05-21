import json
import os
from collections import defaultdict
from metadata_logger import log_pipeline_run

WAREHOUSE_PATH = os.path.expanduser('~/fintech-data-platform/warehouse')
BRONZE_PATH = f'{WAREHOUSE_PATH}/bronze/transactions'
SILVER_PATH = f'{WAREHOUSE_PATH}/silver'
GOLD_PATH = f'{WAREHOUSE_PATH}/gold'

os.makedirs(SILVER_PATH, exist_ok=True)
os.makedirs(GOLD_PATH, exist_ok=True)

# --- BRONZE → SILVER ---
print("Reading Bronze layer...")
all_records = []

for filename in os.listdir(BRONZE_PATH):
    if filename.endswith('.json'):
        with open(f'{BRONZE_PATH}/{filename}') as f:
            all_records.extend(json.load(f))

print(f"Total records read from Bronze: {len(all_records)}")

seen_ids = set()
silver_records = []
for record in all_records:
    if record['transaction_id'] not in seen_ids:
        seen_ids.add(record['transaction_id'])
        silver_records.append(record)

print(f"After deduplication (Silver): {len(silver_records)} records")

silver_file = f'{SILVER_PATH}/transactions_clean.json'
with open(silver_file, 'w') as f:
    json.dump(silver_records, f, indent=2)

# Log Bronze → Silver metadata
log_pipeline_run(
    layer="Silver",
    source="Bronze Layer (local warehouse/bronze/transactions)",
    destination="Silver Layer (local warehouse/silver/transactions_clean.json)",
    records_in=len(all_records),
    records_out=len(silver_records),
    transformation="Deduplication by transaction_id"
)

# --- SILVER → GOLD ---
print("Calculating Gold layer aggregations...")
metrics = defaultdict(lambda: {'total_volume': 0.0, 'transaction_count': 0})

for record in silver_records:
    category = record['merchant_category']
    metrics[category]['total_volume'] += record['amount']
    metrics[category]['transaction_count'] += 1

gold_records = [
    {
        'merchant_category': cat,
        'total_volume': round(data['total_volume'], 2),
        'transaction_count': data['transaction_count']
    }
    for cat, data in metrics.items()
]

gold_file = f'{GOLD_PATH}/merchant_metrics.json'
with open(gold_file, 'w') as f:
    json.dump(gold_records, f, indent=2)

# Log Silver → Gold metadata
log_pipeline_run(
    layer="Gold",
    source="Silver Layer (local warehouse/silver/transactions_clean.json)",
    destination="Gold Layer (local warehouse/gold/merchant_metrics.json)",
    records_in=len(silver_records),
    records_out=len(gold_records),
    transformation="Aggregation by merchant_category (total_volume, transaction_count)"
)

print("Gold Layer - Merchant Metrics:")
print("-" * 45)
for row in sorted(gold_records, key=lambda x: x['total_volume'], reverse=True):
    print(f"  {row['merchant_category']:<15} | "
          f"Volume: {row['total_volume']:>10.2f} | "
          f"Count: {row['transaction_count']}")
