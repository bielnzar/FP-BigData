#!/bin/bash

STRUCTURED_DATA_TOPIC="global-health-stats"
STRUCTURED_CONSUMER_GROUP="health-stats-group"
STRUCTURED_CONSUMER_BATCH_SIZE=100000

UNSTRUCTURED_DATA_TOPIC="medical-abstracts"
UNSTRUCTURED_CONSUMER_GROUP="abstracts-group"
UNSTRUCTURED_CONSUMER_BATCH_SIZE=100

CONSUMER_BATCH_TIMEOUT=30

echo "Stopping previous consumer processes if any..."
pkill -f "src/consumer/consumer.py"
sleep 2

echo "Starting Consumer for Structured Data (Topic: $STRUCTURED_DATA_TOPIC)..."
python src/consumer/consumer.py \
    --topic "$STRUCTURED_DATA_TOPIC" \
    --group "$STRUCTURED_CONSUMER_GROUP" \
    --batch-size "$STRUCTURED_CONSUMER_BATCH_SIZE" \
    --batch-timeout "$CONSUMER_BATCH_TIMEOUT" &
CONSUMER_STRUCTURED_PID=$!
echo "Consumer for Structured Data started with PID: $CONSUMER_STRUCTURED_PID"

echo "Starting Consumer for Semi-Structured Data (Topic: $UNSTRUCTURED_DATA_TOPIC)..."
python src/consumer/consumer.py \
    --topic "$UNSTRUCTURED_DATA_TOPIC" \
    --group "$UNSTRUCTURED_CONSUMER_GROUP" \
    --batch-size "$UNSTRUCTURED_CONSUMER_BATCH_SIZE" \
    --batch-timeout "$CONSUMER_BATCH_TIMEOUT" &
CONSUMER_UNSTRUCTURED_PID=$!
echo "Consumer for Semi-Structured Data started with PID: $CONSUMER_UNSTRUCTURED_PID"

echo ""
echo "All consumer processes started."
echo "Consumers will run continuously."
echo "To stop them, use 'kill <PID>' or 'pkill -f src/consumer/consumer.py'."
echo "Monitor MinIO console (http://localhost:9001) for incoming files in the 'bronze' bucket."
