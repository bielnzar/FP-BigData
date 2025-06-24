import os
import json
import time
import argparse
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT_URL_HOST", "http://localhost:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_NAME = "bronze"

def create_minio_client():
    """Membuat dan mengembalikan klien MinIO."""
    print(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        found = client.bucket_exists(MINIO_BUCKET_NAME)
        if not found:
            print(f"Bucket '{MINIO_BUCKET_NAME}' not found. Please ensure it's created.")
            return None
        print("Successfully connected to MinIO and bucket found.")
        return client
    except S3Error as e:
        print(f"Error connecting to MinIO: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred with MinIO client: {e}")
        return None

def upload_batch_to_minio(client, bucket, topic, messages):
    """Mengunggah satu batch pesan ke MinIO."""
    if not messages:
        return
    
    try:
        batch_content = "\n".join(json.dumps(msg) for msg in messages)
        batch_bytes = batch_content.encode('utf-8')
        
        timestamp = int(time.time() * 1000)
        object_name = f"{topic}/{timestamp}_{len(messages)}_rows.jsonl"
        
        client.put_object(
            bucket,
            object_name,
            data=BytesIO(batch_bytes),
            length=len(batch_bytes),
            content_type='application/jsonl'
        )
        print(f"Successfully uploaded batch of {len(messages)} messages to MinIO: {object_name}")
    except S3Error as e:
        print(f"Error uploading to MinIO: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")

def consume_and_upload(topic, group_id, batch_size, batch_timeout):
    """Mengkonsumsi pesan dari Kafka dan mengunggahnya dalam batch ke MinIO."""
    minio_client = create_minio_client()
    if not minio_client:
        return

    print(f"Connecting to Kafka broker at {KAFKA_BROKER_URL} for topic '{topic}'...")
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER_URL,
            auto_offset_reset='earliest',
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Successfully connected to Kafka.")
    except KafkaError as e:
        print(f"Error connecting to Kafka: {e}")
        return

    messages = []
    last_message_time = time.time()
    print(f"Waiting for messages... Target Batch size: {batch_size}, Idle Timeout: {batch_timeout}s")
    
    try:
        while True:
            records_batch = consumer.poll(timeout_ms=1000, max_records=1000)
            
            if records_batch:
                last_message_time = time.time()
                for tp, records in records_batch.items():
                    for record in records:
                        messages.append(record.value)

            time_since_last_message = time.time() - last_message_time
            
            if len(messages) >= batch_size:
                print(f"Batch size target reached ({len(messages)}/{batch_size}). Uploading...")
                upload_batch_to_minio(minio_client, MINIO_BUCKET_NAME, topic, messages)
                messages.clear()
                last_message_time = time.time()
            elif messages and time_since_last_message >= batch_timeout:
                print(f"Data stream idle timeout reached ({int(time_since_last_message)}s >= {batch_timeout}s). Uploading {len(messages)} messages...")
                upload_batch_to_minio(minio_client, MINIO_BUCKET_NAME, topic, messages)
                messages.clear()
                last_message_time = time.time()

    except KeyboardInterrupt:
        print("\nConsumer stopped by user. Uploading any remaining messages...")
        if messages:
            upload_batch_to_minio(minio_client, MINIO_BUCKET_NAME, topic, messages)
    finally:
        consumer.close()
        print("Kafka consumer closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer to MinIO Uploader")
    parser.add_argument("--topic", required=True, help="Kafka topic to consume from.")
    parser.add_argument("--group", required=True, help="Kafka consumer group ID.")
    parser.add_argument("--batch-size", type=int, required=True, help="Number of messages per batch file.")
    parser.add_argument("--batch-timeout", type=int, required=True, help="Timeout in seconds to wait for a full batch.")
    
    args = parser.parse_args()
    
    consume_and_upload(args.topic, args.group, args.batch_size, args.batch_timeout)