#!/bin/bash

# Skrip untuk mereset (menghapus dan membuat ulang) topik Kafka yang digunakan dalam proyek.
# Jalankan skrip ini dari direktori root proyek.

# --- Konfigurasi ---
KAFKA_CONTAINER_NAME="kafka_server" # Sesuaikan jika nama kontainer Kafka Anda berbeda di docker-compose.yml
BOOTSTRAP_SERVER="localhost:9092" # Alamat internal Kafka di dalam kontainer

# Daftar topik yang akan direset
TOPIC_STRUCTURED="structured_health_stats_raw"
TOPIC_UNSTRUCTURED="unstructured_text_data_raw"

# --- Fungsi untuk eksekusi perintah ---

# Fungsi untuk menghapus topik
delete_topic() {
    local topic_name=$1
    echo "INFO: Attempting to delete topic '$topic_name'..."
    docker exec -it $KAFKA_CONTAINER_NAME kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --delete \
        --topic $topic_name
    # Perintah delete mungkin gagal jika topik tidak ada, itu tidak masalah.
}

# Fungsi untuk membuat topik
create_topic() {
    local topic_name=$1
    echo "INFO: Creating topic '$topic_name' with 1 partition and 1 replica..."
    docker exec -it $KAFKA_CONTAINER_NAME kafka-topics \
        --bootstrap-server $BOOTSTRAP_SERVER \
        --create \
        --topic $topic_name \
        --partitions 1 \
        --replication-factor 1
}

# --- Eksekusi Utama ---

echo "--- Starting Kafka Topic Reset ---"

# Hapus topik lama
delete_topic $TOPIC_STRUCTURED
delete_topic $TOPIC_UNSTRUCTURED

# Beri jeda singkat agar Kafka sempat memproses penghapusan
echo "INFO: Waiting for 5 seconds for topics to be fully deleted..."
sleep 5

# Buat topik baru
create_topic $TOPIC_STRUCTURED
create_topic $TOPIC_UNSTRUCTURED

# Verifikasi hasil
echo "
INFO: Verifying topics...
Listing all topics in Kafka:"
docker exec -it $KAFKA_CONTAINER_NAME kafka-topics \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --list

echo "
--- Kafka Topic Reset Finished ---"