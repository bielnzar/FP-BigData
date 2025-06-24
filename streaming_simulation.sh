#!/bin/bash

echo "Starting Docker services (Kafka, MinIO)..."
docker compose up -d
echo "Waiting for services to stabilize..."
sleep 15

echo ""
echo "--- Starting Producers ---"

chmod +x start_producers.sh
./start_producers.sh

sleep 5

echo ""
echo "--- Starting Consumers ---"

chmod +x start_consumers.sh
./start_consumers.sh

echo ""
echo "All processes started."
echo "Producers will run until files are fully read."
echo "Consumers will run continuously."
echo "To stop all background processes, you can run 'pkill -f python' or 'pkill -f producer.py; pkill -f consumer.py'."
echo ""
echo "Monitor MinIO console (http://localhost:9001) for incoming files in the 'bronze' bucket."
echo "Check logs with 'docker compose logs -f kafka' or 'docker compose logs -f minio'."