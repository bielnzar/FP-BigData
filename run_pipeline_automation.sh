#!/bin/bash

# Menghentikan eksekusi jika terjadi kesalahan
set -e

SPARK_JOB_INTERVAL_SECONDS=3600

cleanup() {
    echo -e "\n\nSkrip dihentikan. Menghentikan proses konsumen..."
    pkill -f "src/consumer/consumer.py" || true
    echo "Proses konsumen telah dihentikan."
    echo "Untuk mematikan semua layanan Docker, jalankan: docker compose down"
    exit 0
}

trap cleanup SIGINT

echo "======================================================"
echo "Langkah 1: Memulai semua layanan dengan Docker Compose..."
echo "======================================================"
docker compose up -d
echo "Menunggu 20 detik agar semua layanan (terutama Kafka & Spark) siap..."
sleep 20

echo "======================================================"
echo "Langkah 2: Memulai konsumen Kafka di latar belakang..."
echo "======================================================"
bash src/consumer/start_consumers.sh
echo "Konsumen telah dimulai dan akan terus berjalan untuk menerima data."

echo "======================================================"
echo "Langkah 3: Memulai loop otomatisasi untuk pekerjaan Spark."
echo "Pekerjaan akan dijalankan setiap $SPARK_JOB_INTERVAL_SECONDS detik."
echo "Tekan Ctrl+C untuk menghentikan skrip ini dengan aman."
echo "======================================================"

while true; do
    echo -e "\n\n----- Memulai Siklus Pemrosesan Data Baru -----"
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    echo "Waktu Mulai: $TIMESTAMP"
    
    echo "\n[1/3] Menjalankan: bronze_to_silver.py"
    bash src/spark_jobs/run_spark_job.sh bronze_to_silver.py
    
    echo "\n[2/3] Menjalankan: silver_to_gold.py"
    bash src/spark_jobs/run_spark_job.sh silver_to_gold.py
    
    echo "\n[3/3] Menjalankan: train_model.py (Melatih ulang model dengan data terbaru)"
    bash src/spark_jobs/run_spark_job.sh train_model.py
    
    echo "\n----- Siklus Pemrosesan Data Selesai -----"
    echo "Model dan data mart telah diperbarui."
    echo "Menunggu selama $SPARK_JOB_INTERVAL_SECONDS detik sebelum memulai siklus berikutnya..."
    sleep "$SPARK_JOB_INTERVAL_SECONDS"
done
