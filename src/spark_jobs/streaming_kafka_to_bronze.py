# src/spark_jobs/streaming_kafka_to_bronze.py

import argparse
import yaml # Untuk membaca konfigurasi YAML, perlu pip install PyYAML
from pyspark.sql.functions import col, from_json, current_timestamp

# Import utilitas dan skema
from utils.spark_session_manager import get_spark_session
from utils.schema_definitions import structured_health_stats_schema, unstructured_text_schema

def load_config(config_path="config/job_configs.yaml"):
    """Memuat konfigurasi dari file YAML."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        # Ganti placeholder bucket jika ada (cara sederhana)
        # Di produksi, Anda mungkin menggunakan Jinja2 atau cara lain yang lebih canggih
        minio_bucket_name = config.get("minio_bucket", "fp-bigdata") # Default jika tidak ada
        for job_key, job_config in config.items():
            if isinstance(job_config, dict):
                for path_key, path_value in job_config.items():
                    if isinstance(path_value, str) and "{{minio_bucket}}" in path_value:
                        job_config[path_key] = path_value.replace("{{minio_bucket}}", minio_bucket_name)
        return config
    except FileNotFoundError:
        print(f"Warning: Config file not found at {config_path}. Using default or hardcoded values.")
        return {} # Kembalikan dict kosong jika file tidak ditemukan
    except Exception as e:
        print(f"Error loading config: {e}")
        return {}


def process_stream(spark, kafka_bootstrap_servers, input_topic, schema, output_path_s3a, checkpoint_location_s3a, trigger_interval="1 minute"):
    """
    Fungsi generik untuk memproses satu stream dari Kafka ke Delta Lake Bronze.
    """
    print(f"Starting stream for topic: {input_topic}")
    print(f"Output path: {output_path_s3a}")
    print(f"Checkpoint location: {checkpoint_location_s3a}")

    # Baca dari Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")  # Baca dari awal untuk development/batch pertama
        # .option("failOnDataLoss", "false") # Pertimbangkan ini untuk produksi
        .load()
    )

    # Decode pesan Kafka (asumsi value adalah JSON string)
    # dan terapkan skema yang telah ditentukan
    processed_df = (
        kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
            # Anda bisa menyertakan key, topic, partition, offset, timestamp Kafka jika perlu
            # col("key").cast("string").alias("kafka_key"),
            # col("topic").alias("kafka_topic"),
            # col("partition").alias("kafka_partition"),
            # col("offset").alias("kafka_offset"),
            # col("timestamp").alias("kafka_timestamp") # Timestamp event Kafka
        )
        .select("data.*") # Flatten struct 'data' menjadi kolom-kolom
        .withColumn("ingest_timestamp", current_timestamp()) # Tambahkan timestamp ingest
    )

    # Tulis stream ke Delta Lake di MinIO
    # Gunakan .trigger(processingTime=...) untuk interval micro-batch
    # atau .trigger(availableNow=True) untuk memproses semua data yang tersedia lalu berhenti (cocok untuk batch-like job)
    # atau .trigger(continuous=...) untuk low-latency continuous processing (lebih kompleks)
    query = (
        processed_df.writeStream.format("delta")
        .outputMode("append")
        .option("path", output_path_s3a)
        .option("checkpointLocation", checkpoint_location_s3a)
        .trigger(processingTime=trigger_interval) # Atau availableNow=True untuk satu kali proses
        .start()
    )
    
    print(f"Stream for topic {input_topic} started. Writing to {output_path_s3a}")
    return query


def main():
    parser = argparse.ArgumentParser(description="Spark Streaming Job from Kafka to Bronze Delta Lake")
    parser.add_argument("--config_path", default="src/spark_jobs/config/job_configs.yaml", help="Path to the YAML config file")
    args = parser.parse_args()

    # Muat konfigurasi
    # Untuk menjalankan dari root proyek: python src/spark_jobs/streaming_kafka_to_bronze.py
    # Jika Anda menjalankan dari `src/spark_jobs/` maka path config adalah `config/job_configs.yaml`
    # Asumsi dijalankan dari root proyek atau path disesuaikan
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir)) # Mundur 2 level
    default_config_path = os.path.join(project_root, "src/spark_jobs/config/job_configs.yaml")

    config_file_path = args.config_path if os.path.isabs(args.config_path) else os.path.join(project_root, args.config_path)
    if not os.path.exists(config_file_path):
        config_file_path = default_config_path # Fallback ke default jika path argumen tidak ada
        print(f"Argument config_path not found, using default: {config_file_path}")

    config = load_config(config_file_path)
    
    if not config:
        print("Failed to load configuration. Exiting.")
        return

    job_config = config.get("kafka_to_bronze_job", {})
    common_kafka_bootstrap = config.get("kafka_bootstrap_servers", "kafka:9092") # Ambil dari config umum

    # Dapatkan SparkSession
    spark = get_spark_session("KafkaToBronzeStreaming")

    if not spark:
        print("Failed to initialize SparkSession. Exiting.")
        return

    # Path dan topik dari konfigurasi
    structured_topic = job_config.get("structured_data_topic", "structured_health_stats_raw")
    unstructured_topic = job_config.get("unstructured_data_topic", "unstructured_text_data_raw")
    
    bronze_structured_path = job_config.get("bronze_structured_path_s3a")
    bronze_unstructured_path = job_config.get("bronze_unstructured_path_s3a")
    
    checkpoint_structured = job_config.get("checkpoint_location_structured_s3a")
    checkpoint_unstructured = job_config.get("checkpoint_location_unstructured_s3a")
    
    trigger_interval = job_config.get("trigger_interval", "1 minute")

    if not all([bronze_structured_path, bronze_unstructured_path, checkpoint_structured, checkpoint_unstructured]):
        print("Error: Missing S3A paths or checkpoint locations in configuration.")
        spark.stop()
        return

    # Mulai stream untuk data terstruktur
    query_structured = process_stream(
        spark,
        common_kafka_bootstrap,
        structured_topic,
        structured_health_stats_schema, # Gunakan skema yang telah didefinisikan
        bronze_structured_path,
        checkpoint_structured,
        trigger_interval
    )

    # Mulai stream untuk data tidak terstruktur
    query_unstructured = process_stream(
        spark,
        common_kafka_bootstrap,
        unstructured_topic,
        unstructured_text_schema, # Gunakan skema yang telah didefinisikan
        bronze_unstructured_path,
        checkpoint_unstructured,
        trigger_interval
    )

    # Tunggu semua stream selesai (atau salah satu gagal)
    # Untuk aplikasi streaming yang berjalan terus menerus, ini akan berjalan selamanya
    # spark.streams.awaitAnyTermination() 
    # Jika menggunakan trigger(availableNow=True), query akan berhenti setelah data diproses
    # Anda mungkin perlu logika berbeda untuk menunggu query yang `availableNow=True`
    
    # Untuk trigger(processingTime=...) kita bisa biarkan berjalan atau tambahkan awaitAnyTermination
    # Jika Anda ingin job ini berjalan sebentar saja untuk tes (misalnya dengan availableNow=True)
    # maka Anda perlu .awaitTermination() pada setiap query atau logika lain.
    # Untuk contoh ini, kita asumsikan bisa berjalan terus atau akan dihentikan manual.
    
    print("All streams started. Awaiting termination or manual stop.")
    try:
        # Ini akan membuat aplikasi utama tetap berjalan sampai salah satu query dihentikan atau gagal.
        # Atau jika Anda menjalankan dengan trigger(availableNow=True), ini mungkin tidak diperlukan
        # dan Anda cukup `query_structured.awaitTermination()` dan `query_unstructured.awaitTermination()`
        # jika Anda ingin memastikan keduanya selesai.
        
        # Jika menggunakan processingTime, biarkan ini atau atur timeout
        spark.streams.awaitAnyTermination()
        
    except KeyboardInterrupt:
        print("Streaming job manually interrupted.")
    finally:
        print("Stopping SparkSession...")
        # Hentikan query secara eksplisit jika masih aktif (opsional, Spark biasanya handle)
        if query_structured and query_structured.isActive:
            query_structured.stop()
        if query_unstructured and query_unstructured.isActive:
            query_unstructured.stop()
        spark.stop()
        print("SparkSession stopped.")

if __name__ == "__main__":
    main()