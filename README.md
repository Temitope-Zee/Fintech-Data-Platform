# Fintech Data Platform

A portfolio-grade end-to-end real-time data engineering pipeline that processes,
validates and stores streaming financial transactions using a Medallion Lakehouse
architecture.

---
## Architecture

The pipeline is broken into four phases:

- Phase 1 — Mock data generator produces fake financial transactions
- Phase 2 — Apache Kafka receives and queues all transactions
- Phase 3 — PySpark and Great Expectations validates and routes records
- Phase 4 — Medallion Lakehouse stores Bronze, Silver and Gold layers

![Architecture Diagram](images/architecture.png)

---## Dashboard

![Pipeline Dashboard](images/dashboard.png)

---

## Tech Stack

- Infrastructure: Docker and Docker Compose
- Message Broker: Apache Kafka and Zookeeper
- Stream Processing: PySpark Structured Streaming
- Data Quality: Great Expectations
- Orchestration: Apache Airflow
- Local Storage: MinIO (S3-compatible object store)
- Cloud Storage: AWS S3
- Database: PostgreSQL
- Lineage Tracking: Custom Metadata Logger

---
## Medallion Architecture

| Layer | Description | Storage |
|---|---|---|
| Bronze | Raw validated transactions | MinIO + AWS S3 |
| Silver | Deduplicated clean records | MinIO |
| Gold | Aggregated merchant metrics | MinIO |

---
## Project Structure

fintech-data-platform/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
├── src/
│   ├── generator/
│   │   └── generator.py
│   ├── streaming/
│   │   └── streaming_job.py
│   └── lakehouse/
│       ├── bronze_ingestion.py
│       ├── silver_gold_etl.py
│       ├── minio_silver_gold_etl.py
│       └── metadata_logger.py
├── images/
│   ├── architecture.png
│   └── dashboard.png
└── warehouse/
    ├── bronze/
    ├── silver/
    └── gold/

---
## How to Run

### 1. Clone the Repository
git clone https://github.com/Temitope-Zee/Fintech-Data-Platform.git
cd Fintech-Data-Platform

### 2. Set Up Environment Variables
Create a .env file with your AWS credentials:
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_REGION=eu-north-1
S3_BUCKET=your_bucket_name

### 3. Start the Full Pipeline
docker compose up -d

This single command starts all 6 services:
- Zookeeper
- Kafka
- MinIO (local S3)
- Generator
- Validator
- Bronze ingestion

### 4. Open MinIO Dashboard
Go to http://localhost:9001
Username: minioadmin
Password: minioadmin123

### 5. Run Silver and Gold ETL
python src/lakehouse/minio_silver_gold_etl.py

### 6. Start Airflow for Automated Scheduling
export AIRFLOW_HOME=~/airflow
airflow scheduler
airflow webserver --port 8080

Then go to http://localhost:8080 to monitor your pipeline.

---
## Pipeline Services

| Service | Role | Port |
|---|---|---|
| Zookeeper | Kafka coordinator | 2181 |
| Kafka | Message broker | 29092 |
| MinIO | Local S3 storage | 9000/9001 |
| Generator | Fake transaction producer | - |
| Validator | Data quality checker | - |
| Bronze | Lakehouse ingestion | - |

---
## Key Features

- Real-time data generation with Faker producing 2 transactions per second
- Stream validation routing valid records to clean_transactions topic
- Invalid records routed to dead_letter_queue for auditing
- Bronze Silver Gold medallion lakehouse on MinIO and AWS S3
- Apache Airflow scheduling ETL every hour automatically
- Metadata lineage tracking per pipeline run
- Full Docker containerisation — runs with one command

---
## Next Steps

- Integrate Apache Iceberg for ACID transactions on the lakehouse
- Add DataHub for full data lineage tracking
- Build a real-time dashboard with Apache Superset
- Add dbt for data transformations

---

## Author

Zainab Olubuade — Data Engineering Portfolio Project

GitHub: https://github.com/Temitope-Zee/Fintech-Data-Platform
