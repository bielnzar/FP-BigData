from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import json
import time
import os
from datetime import datetime
from io import BytesIO
import logging

# --- Konfigurasi Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s',
)
logger = logging.getLogger("UnstructuredConsumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKERS_HOST', 'localhost:29092') 
KAFKA_TOPIC = 'unstructured_text_data_raw'
KAFKA_CONSUMER_GROUP = 'unstructured-minio-bronze-writer-v2'

# MinIO Configuration
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'bosmuda'
MINIO_SECRET_KEY = 'Kelompok6'
MINIO_BUCKET_NAME = 'fp-bigdata'
MINIO_BRONZE_PATH_PREFIX = 'bronze/unstructured_data'

# Batching Configuration
UNSTRUCTURED_BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 300

consumer = None
minio_client = None

def initialize_clients():
    global consumer, minio_client
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            group_id=KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            enable_auto_commit=False,
            consumer_timeout_ms=10000
        )
        logger.info(f"Connected to Kafka topic '{KAFKA_TOPIC}'.")
    except Exception as e:
        logger.error(f"Could not connect to Kafka for topic '{KAFKA_TOPIC}': {e}", exc_info=True)
        exit()

    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        logger.info(f"MinIO Client connected successfully to {MINIO_ENDPOINT}.")
        if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
            logger.warning(f"Bucket '{MINIO_BUCKET_NAME}' does not exist in MinIO. Please create it.")
            # minio_client.make_bucket(MINIO_BUCKET_NAME) 
    except Exception as e:
        logger.error(f"Could not connect to MinIO: {e}", exc_info=True)
        exit()

def write_batch_to_minio(messages_batch, object_prefix, attempt=0):
    """Menulis batch pesan sebagai satu file JSON Lines ke MinIO."""
    if not messages_batch:
        return True
    object_name = ""
    try:
        now = datetime.now()
        object_name = f"{object_prefix}/{now.strftime('%Y/%m/%d')}/batch_{now.strftime('%H%M%S%f')}.jsonl"
        jsonl_content = "\n".join([json.dumps(msg_data) for msg_data in messages_batch])
        jsonl_bytes = jsonl_content.encode('utf-8')
        jsonl_data_size = len(jsonl_bytes)
        jsonl_stream = BytesIO(jsonl_bytes)
        minio_client.put_object(
            MINIO_BUCKET_NAME, object_name, jsonl_stream,
            length=jsonl_data_size, content_type='application/x-jsonlines'
        )
        return True
    except S3Error as s3_err:
        logger.error(f"S3Error writing batch to {object_name} (attempt {attempt+1}): {s3_err}", exc_info=True)
        if attempt < 2:
            time.sleep(2 ** attempt)
            return write_batch_to_minio(messages_batch, object_prefix, attempt + 1)
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing batch to {object_name}: {e}", exc_info=True)
        return False

def main():
    """Fungsi utama untuk menjalankan loop consumer Kafka."""
    global consumer
    logger.info("==========================================================")
    logger.info(f"Starting consumer for topic '{KAFKA_TOPIC}'...")
    logger.info(f"BATCH_SIZE={UNSTRUCTURED_BATCH_SIZE}, TIMEOUT={BATCH_TIMEOUT_SECONDS}s")
    logger.info("==========================================================")
    message_batch_values = []
    last_batch_write_time = time.time()
    processed_count_session = 0

    try:
        while True:
            batch_from_poll = consumer.poll(
                timeout_ms=1000, 
                max_records=UNSTRUCTURED_BATCH_SIZE - len(message_batch_values) if message_batch_values else UNSTRUCTURED_BATCH_SIZE
            )

            if not batch_from_poll:
                if message_batch_values and (time.time() - last_batch_write_time) >= BATCH_TIMEOUT_SECONDS:
                    logger.info(f"Batch timeout reached. Writing {len(message_batch_values)} pending messages.")
                    if write_batch_to_minio(message_batch_values, MINIO_BRONZE_PATH_PREFIX):
                        consumer.commit() # Commit offset setelah batch berhasil
                        logger.info(f"Kafka offsets committed. Batch count: {len(message_batch_values)}")
                        message_batch_values = []
                        last_batch_write_time = time.time()
                    else:
                        logger.error("Failed to write timed-out batch to MinIO. Messages remain in batch for next attempt.")
                        time.sleep(5)
                continue 

            # Proses pesan dari poll
            for tp, records in batch_from_poll.items():
                for record in records:
                    message_batch_values.append(record.value)
                    processed_count_session += 1

            # Cek apakah batch sudah penuh atau timeout
            batch_full = len(message_batch_values) >= UNSTRUCTURED_BATCH_SIZE
            batch_timeout = (time.time() - last_batch_write_time) >= BATCH_TIMEOUT_SECONDS

            if batch_full or (message_batch_values and batch_timeout):
                reason = "batch full" if batch_full else "batch timeout"
                logger.info(f"FLUSH: Flushing batch due to {reason}. Size: {len(message_batch_values)}")
                if write_batch_to_minio(message_batch_values, MINIO_BRONZE_PATH_PREFIX):
                    consumer.commit() # Commit offset setelah batch berhasil ditulis
                    logger.info(f"SUCCESS: Wrote batch to MinIO and committed offsets. Batch size: {len(message_batch_values)}")
                    message_batch_values = []
                    last_batch_write_time = time.time()
                else:
                    logger.error(f"FAILURE: Could not write batch to MinIO. Offsets not committed. Retrying...")
                    time.sleep(5)
    
    except KeyboardInterrupt:
        logger.info("\n------------------ SHUTTING DOWN ------------------")
        if message_batch_values:
            logger.info(f"Writing final batch of {len(message_batch_values)} messages...")
            if write_batch_to_minio(message_batch_values, MINIO_BRONZE_PATH_PREFIX):
                logger.info("Final batch written successfully.")
                consumer.commit()
                logger.info("Final offsets committed.")
            else:
                logger.error("Failed to write final batch to MinIO during shutdown.")
            
    except Exception as e:
        logger.error(f"FATAL: An unhandled exception occurred in the consumer loop: {e}", exc_info=True)
    
    finally:
        if consumer:
            logger.info("Closing Kafka consumer.")
            consumer.close()
        logger.info(f"Consumer shut down. Total messages processed in this session: {processed_count_session}")
        logger.info("==========================================================")

if __name__ == "__main__":
    initialize_clients()
    main()