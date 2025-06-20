from kafka import KafkaProducer
import json
import csv
import time
import os
import argparse 

KAFKA_BROKER_URL = 'localhost:29092'
TOPIC_NAME = 'structured_health_stats_raw'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FILE_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Global Health Statistics.csv') # Nama file sesuai contoh
# MAX_MESSAGES_TO_SEND = 10000

def clean_column_name(name):
    """Membersihkan nama kolom agar valid sebagai kunci JSON dan nama kolom Spark."""
    name = name.replace(' (%)', '_Percent')
    name = name.replace(' (USD)', '_USD')
    name = name.replace(' per 1000', '_per_1000')
    name = name.replace(' in 5 Years', '_in_5_Years')
    name = name.replace('/', '_') # Ganti '/' dengan '_'
    name = name.replace(' ', '_')
    name = name.replace('(', '')
    name = name.replace(')', '')
    return name

def connect_kafka_producer(broker_url):
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=broker_url,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Successfully connected to Kafka broker at {broker_url} for topic {TOPIC_NAME}")
    except Exception as e:
        print(f"Exception while connecting Kafka: {e}")
    return _producer

def publish_messages(producer_instance, topic_name, file_path, max_messages):
    message_count = 0
    try:
        with open(file_path, mode='r', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            # Dapatkan header asli dan bersihkan untuk digunakan sebagai kunci JSON
            cleaned_fieldnames = [clean_column_name(field) for field in csv_reader.fieldnames]
            
            for i, row_dict in enumerate(csv_reader):
                message = {clean_column_name(k): v for k, v in row_dict.items()}
                
                producer_instance.send(topic_name, value=message)
                message_count += 1

                # Cetak progres setiap 10000 pesan
                if message_count % 10000 == 0:
                    print(f"Sent {message_count} messages...")

                if max_messages > 0 and message_count >= max_messages:
                    print(f"\nReached max messages to send ({max_messages}). Stopping.")
                    break
                # time.sleep(0.01)

        producer_instance.flush()
        print(f"\nSuccessfully published {message_count} messages to topic: {topic_name}")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {file_path}")
    except Exception as e:
        print(f"An error occurred during publishing: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Kafka Publisher for topic {TOPIC_NAME}")
    parser.add_argument(
        "--max-messages",
        type=int,
        default=1000000,
        help="Maximum number of messages to send. Set to 0 or a negative number to send all. Default: 1000."
    )
    args = parser.parse_args()

    print(f"Starting Kafka Publisher for structured data (Topic: {TOPIC_NAME})...")
    kafka_producer = connect_kafka_producer(KAFKA_BROKER_URL)
    if kafka_producer:
        publish_messages(kafka_producer, TOPIC_NAME, CSV_FILE_PATH, args.max_messages)
        if kafka_producer:
            kafka_producer.close()
            print("Kafka producer closed.")
    else:
        print("Could not connect to Kafka. Exiting.")