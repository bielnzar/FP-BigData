import csv
import json
import time
import os
import argparse
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BOOTSTRAP_SERVERS_HOST", "localhost:9092")

def create_producer():
    """Create Kafka Producer."""
    print(f"Connecting to Kafka broker at {KAFKA_BROKER_URL}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # acks='all',
            # retries=3,
        )
        print("Successfully connected to Kafka broker.")
        return producer
    except KafkaError as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def publish_csv_to_kafka(producer, topic_name, file_path, rows_per_second=None):
    """
    Membaca file CSV dan mempublikasikan setiap baris sebagai pesan JSON ke Kafka.
    """
    if not producer:
        print("Producer is not initialized. Exiting.")
        return

    print(f"Publishing data from '{file_path}' to Kafka topic '{topic_name}'...")
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            count = 0
            for row in csv_reader:
                try:
                    future = producer.send(topic_name, value=row)
                    # print(f"Sent: {row} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                    count += 1
                    if count % 100 == 0:
                        print(f"Sent {count} rows to Kafka topic '{topic_name}'...")
                    
                    if rows_per_second and rows_per_second > 0:
                        time.sleep(1.0 / rows_per_second)

                except KafkaError as e:
                    print(f"Error sending message: {row} - {e}")
                except Exception as e:
                    print(f"An unexpected error occurred while sending message: {row} - {e}")
            
            producer.flush()
            print(f"Finished publishing {count} rows from '{file_path}' to Kafka topic '{topic_name}'.")

    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka CSV Producer")
    parser.add_argument("--file", required=True, help="Path to the CSV file to produce.")
    parser.add_argument("--topic", required=True, help="Kafka topic to produce messages to.")
    parser.add_argument("--rps", type=int, default=0, help="Rows per second to send (0 for no limit).") # 0 atau tidak diset berarti secepat mungkin
    
    args = parser.parse_args()

    kafka_producer = create_producer()
    
    if kafka_producer:
        publish_csv_to_kafka(kafka_producer, args.topic, args.file, args.rps)
        kafka_producer.close()
        print("Kafka producer closed.")