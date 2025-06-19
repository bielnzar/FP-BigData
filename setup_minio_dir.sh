#!/binbash

# Skrip untuk membuat struktur direktori awal di MinIO

# Variabel
MINIO_ALIAS="local" # Ganti dengan alias MinIO Anda jika berbeda
MAIN_BUCKET="fp-bigdata" # Ganti dengan nama bucket utama Anda

# Fungsi untuk membuat direktori jika belum ada
# mc mb tidak akan error jika direktori sudah ada, jadi ini aman dijalankan ulang
create_dir_if_not_exists() {
    local full_path="$1"
    echo "Creating MinIO directory (object prefix): ${full_path} ..."
    mc mb "${full_path}"
    # Anda bisa menambahkan pengecekan error jika mc mb gagal untuk alasan selain sudah ada
    # if [ $? -ne 0 ]; then
    #     echo "Error creating ${full_path}"
    #     # exit 1 # Uncomment jika ingin skrip berhenti jika ada error
    # fi
}

echo "Starting MinIO directory setup for alias: ${MINIO_ALIAS}, bucket: ${MAIN_BUCKET}"

# 1. Pastikan bucket utama ada (mc mb juga bisa membuat bucket)
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}"

# 2. Buat direktori lapisan Medallion
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/bronze"
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/silver"
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/gold"

# 3. (Opsional) Buat sub-direktori untuk data terstruktur dan tidak terstruktur di setiap lapisan
# Bronze Layer
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/bronze/structured_data"
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/bronze/unstructured_data"

# Silver Layer
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/silver/structured_data_cleaned"
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/silver/unstructured_data_processed"

# Gold Layer
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/gold/structured_aggregates"
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/gold/unstructured_insights"
# atau nama yang lebih spesifik seperti:
# create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/gold/health_metrics_summary"
# create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/gold/text_analysis_output"


# 4. Buat direktori untuk models
create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/models"
# (Opsional) Sub-direktori untuk model spesifik jika sudah tahu namanya
# create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/models/health_predictor"

# 5. (Opsional) Direktori untuk logs jika Spark/aplikasi lain akan menulis log ke MinIO
# create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/logs"
# create_dir_if_not_exists "${MINIO_ALIAS}/${MAIN_BUCKET}/logs/spark_app_logs"

echo "MinIO directory setup finished."
echo "Current structure in ${MINIO_ALIAS}/${MAIN_BUCKET}:"
mc tree "${MINIO_ALIAS}/${MAIN_BUCKET}"