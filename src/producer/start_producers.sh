#!/bin/bash

STRUCTURED_DATA_FILE="data/Global Health Statistics.csv"
STRUCTURED_DATA_TOPIC="global-health-stats"
STRUCTURED_PRODUCER_RPS=0

UNSTRUCTURED_DATA_FILE="data/medical_text_classification_fake_dataset.csv"
UNSTRUCTURED_DATA_TOPIC="medical-abstracts"
UNSTRUCTURED_PRODUCER_RPS=10

echo "Stopping previous producer processes if any..."
pkill -f "src/producer/producer.py"
sleep 2

echo "Clearing and re-creating Kafka topics..."
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic "$STRUCTURED_DATA_TOPIC" > /dev/null 2>&1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --delete --topic "$UNSTRUCTURED_DATA_TOPIC" > /dev/null 2>&1
sleep 5

docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic "$STRUCTURED_DATA_TOPIC" --partitions 1 --replication-factor 1
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic "$UNSTRUCTURED_DATA_TOPIC" --partitions 1 --replication-factor 1
echo "Topics are ready."
sleep 2

echo "Starting Producer for Structured Data (Topic: $STRUCTURED_DATA_TOPIC)..."
python src/producer/producer.py \
    --file "$STRUCTURED_DATA_FILE" \
    --topic "$STRUCTURED_DATA_TOPIC" \
    --rps "$STRUCTURED_PRODUCER_RPS" &
PRODUCER_STRUCTURED_PID=$!
echo "Producer for Structured Data started with PID: $PRODUCER_STRUCTURED_PID"

echo "Starting Producer for Semi-Structured Data (Topic: $UNSTRUCTURED_DATA_TOPIC)..."
python src/producer/producer.py \
    --file "$UNSTRUCTURED_DATA_FILE" \
    --topic "$UNSTRUCTURED_DATA_TOPIC" \
    --rps "$UNSTRUCTURED_PRODUCER_RPS" &
PRODUCER_UNSTRUCTURED_PID=$!
echo "Producer for Semi-Structured Data started with PID: $PRODUCER_UNSTRUCTURED_PID"

echo ""
echo "All producer processes started."
echo "Producers will run until files are fully read."
echo "To stop them, use 'kill <PID>' or 'pkill -f src/producer/producer.py'."
