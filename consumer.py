import json
import boto3
import time
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

# ----------- MINIO CONFIG -----------
MINIO_ENDPOINT = "http://localhost:9001"  # ✅ Corrected port
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
BUCKET_NAME = "bronze-transactions"

# ----------- KAFKA CONFIG -----------
KAFKA_TOPIC = "stock-quotes"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]  # ✅ Adjust this if your Kafka runs in Docker

# ----------- CONNECT TO MINIO -----------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Create bucket if not exists
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' already exists.")
except ClientError:
    s3.create_bucket(Bucket=BUCKET_NAME)
    print(f"Created bucket '{BUCKET_NAME}'.")

# ----------- CONNECT TO KAFKA -----------
print("Connecting to Kafka broker...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer1",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)
print(f"Connected to Kafka topic '{KAFKA_TOPIC}'. Listening for messages...")

# ----------- MAIN LOOP -----------
for message in consumer:
    record = message.value
    symbol = record.get("symbol", "unknown")
    ts = record.get("fetched_at", int(time.time()))
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"✅ Saved record for {symbol} -> s3://{BUCKET_NAME}/{key}")
