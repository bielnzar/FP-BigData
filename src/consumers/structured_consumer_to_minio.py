from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
import json
import time
import os
from datetime import datetime
from io import BytesIO
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("StructuredConsumer")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092' 
KAFKA_TOPIC = 'structured_health_stats_raw'
KAFKA_CONSUMER_GROUP = 'structured-minio-bronze-writer-v2'

# MinIO Configuration
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'bosmuda'
MINIO_SECRET_KEY = 'Kelompok6'
MINIO_BUCKET_NAME = 'fp-bigdata'
MINIO_BRONZE_PATH_PREFIX = 'bronze/structured_data' 

# Batching Configuration 
STRUCTURED_BATCH_SIZE = 100000
BATCH_TIMEOUT_SECONDS = 300

# Inisialisasi Klien
consumer = None
minio_client = None

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id=KAFKA_CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        enable_auto_commit=False, # commit offset secara manual setelah batch berhasil
        consumer_timeout_ms=10000
    )
    logger.info(f"Kafka Consumer for '{KAFKA_TOPIC}' connected successfully.")
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
except Exception as e:
    logger.error(f"Could not connect to MinIO: {e}", exc_info=True)
    exit()

def write_batch_to_minio(batch_data, attempt=0):
    """Menulis batch pesan sebagai satu file JSON Lines ke MinIO."""
    if not batch_data:
        logger.debug("write_batch_to_minio called with empty batch. Skipping.")
        return True

    object_name = "" 
    try:
        now = datetime.now()
        object_name = f"{MINIO_BRONZE_PATH_PREFIX}/{now.strftime('%Y/%m/%d')}/batch_{now.strftime('%H%M%S%f')}_{len(batch_data)}msgs.jsonl"
        
        jsonl_content = "\n".join([json.dumps(msg_value) for msg_value in batch_data])
        jsonl_bytes = jsonl_content.encode('utf-8')
        jsonl_data_size = len(jsonl_bytes)
        jsonl_stream = BytesIO(jsonl_bytes)

        logger.debug(f"Writing batch of {len(batch_data)} msgs to {object_name}")
        minio_client.put_object(
            MINIO_BUCKET_NAME,
            object_name,
            jsonl_stream,
            length=jsonl_data_size,
            content_type='application/x-jsonlines'
        )
        return True
    except S3Error as exc:
        logger.error(f"S3Error writing batch to {object_name} (attempt {attempt+1}): {exc}", exc_info=True)
        if attempt < 2:
            time.sleep(2 ** attempt)
            return write_batch_to_minio(batch_data, attempt + 1)
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing batch to {object_name}: {e}", exc_info=True)
        return False

logger.info("==========================================================")
logger.info(f"Starting consumer for topic '{KAFKA_TOPIC}'...")
logger.info(f"BATCH_SIZE={STRUCTURED_BATCH_SIZE}, TIMEOUT={BATCH_TIMEOUT_SECONDS}s")
logger.info("==========================================================")
message_batch_values = [] # Hanya simpan values untuk ditulis
last_batch_write_time = time.time()
processed_count_session = 0

try:
    while True:
        try:
            batch_from_poll = consumer.poll(timeout_ms=1000, max_records=STRUCTURED_BATCH_SIZE - len(message_batch_values) if message_batch_values else STRUCTURED_BATCH_SIZE)

            if not batch_from_poll:
                if message_batch_values and (time.time() - last_batch_write_time) >= BATCH_TIMEOUT_SECONDS:
                    logger.info(f"TIMEOUT: Flushing {len(message_batch_values)} pending messages.")
                    if write_batch_to_minio(message_batch_values):
                        consumer.commit()
                        logger.info(f"SUCCESS: Wrote batch and committed offsets. Total processed: {processed_count_session}")
                        message_batch_values = []
                        last_batch_write_time = time.time()
                    else:
                        logger.error("FAILURE: Could not write batch to MinIO. Offsets not committed. Retrying...")
                        time.sleep(5)
                continue

            for tp, records in batch_from_poll.items():
                for record in records:
                    # logger.debug(f"Consumed message: Offset={record.offset}, Key={record.key}, Value={str(record.value)[:50]}")
                    message_batch_values.append(record.value)
                    processed_count_session += 1

                # Cek apakah batch sudah penuh setelah menambahkan dari poll
                if len(message_batch_values) >= STRUCTURED_BATCH_SIZE:
                    logger.info(f"BATCH FULL: Flushing {len(message_batch_values)} messages.")
                    if write_batch_to_minio(message_batch_values):
                        consumer.commit()
                        logger.info(f"SUCCESS: Wrote batch and committed offsets. Total processed: {processed_count_session}")
                        message_batch_values = []
                        last_batch_write_time = time.time()
                    else:
                        logger.error("FAILURE: Could not write batch to MinIO. Offsets not committed. Retrying...")
                        time.sleep(5)

        except Exception as poll_exc:
            logger.error(f"Error in consumer poll/processing loop: {poll_exc}", exc_info=True)
            time.sleep(5) 


except KeyboardInterrupt:
    logger.info("\n------------------ SHUTTING DOWN ------------------")
    if message_batch_values:
        logger.info(f"Writing final batch of {len(message_batch_values)} messages due to shutdown.")
        if write_batch_to_minio(message_batch_values):
            consumer.commit() # Commit final
            logger.info("Final batch written and offsets committed.")
        else:
            logger.error("Failed to write final batch to MinIO during shutdown.")
finally:
    if consumer:
        logger.info("Closing Kafka consumer.")
        consumer.close()
    logger.info(f"Consumer shut down. Total messages processed in this session: {processed_count_session}")
    logger.info("==========================================================")