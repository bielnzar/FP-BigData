from kafka import KafkaProducer
import json
import csv
import time
import os
import argparse

KAFKA_BROKER_URL = 'localhost:29092'
TOPIC_NAME = 'unstructured_text_data_raw'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FILE_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'medical_text_classification_fake_dataset.csv')
# MAX_MESSAGES_TO_SEND = 1000

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
            csv_reader = csv.DictReader(csvfile) # Header: text,label
            for i, row in enumerate(csv_reader):
                # Pesan akan langsung menggunakan header 'text' dan 'label'
                message_payload = {
                    "text": row.get('text'), 
                    "label": row.get('label'),
                    "source_document": os.path.basename(file_path)
                    # "original_row_number": i
                }
                
                producer_instance.send(topic_name, value=message_payload)
                message_count += 1

                if message_count % 100 == 0:
                    print(f"Sent {message_count} messages...")
                    print(f"  - Last sent data: {json.dumps(message_payload)}")
                    print("  - Pausing for 3 seconds...")
                    time.sleep(3)

                # Hentikan jika batas maksimum tercapai (jika diset > 0)
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
        default=1000,
        help="Maximum number of messages to send. Set to 0 or a negative number to send all. Default: 1000."
    )
    args = parser.parse_args()

    print(f"Starting Kafka Publisher for unstructured data (Topic: {TOPIC_NAME})...")
    kafka_producer = connect_kafka_producer(KAFKA_BROKER_URL)
    if kafka_producer:
        publish_messages(kafka_producer, TOPIC_NAME, CSV_FILE_PATH, args.max_messages)
        if kafka_producer:
            kafka_producer.close()
            print("Kafka producer closed.")
    else:
        print("Could not connect to Kafka. Exiting.")