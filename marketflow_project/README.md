# MarketFlow – Stock Market Data Pipeline

![Architecture](094e8c01-6755-4ed3-b6cc-6381a3be8ff3.png)

## Overview
MarketFlow is an end-to-end real-time stock market data pipeline featuring:
- API ingestion
- Kafka streaming
- MinIO data lake
- Airflow orchestration
- dbt transformations
- Snowflake warehouse
- Power BI analytics

## Project Structure
```
marketflow_project/
  ├── producer.py
  ├── consumer_raw.py
  ├── docker-compose.yml
  ├── dags/
  ├── dbt/
  └── README.md
```

