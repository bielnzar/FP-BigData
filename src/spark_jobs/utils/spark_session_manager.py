# src/spark_jobs/utils/spark_session_manager.py
import os
from pyspark.sql import SparkSession

def get_spark_session(app_name="MySparkApp"):
    """
    Initializes and returns a SparkSession with configurations for Delta Lake,
    Kafka, and MinIO.
    """
    # ---- MinIO Configuration ----
    # Idealnya, ini dibaca dari environment variables atau file konfigurasi
    # Untuk contoh ini, kita set di sini, tapi JANGAN HARDCODE KREDENSIAL DI PRODUKSI
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "bosmuda") # Ganti dengan env var
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "Kelompok6") # Ganti dengan env var

    # ---- Versi Library (SANGAT PENTING untuk disesuaikan) ----
    # Ganti dengan versi Spark yang Anda gunakan
    spark_version_short = "3.5" # Contoh: "3.4", "3.5"
    # Ganti dengan versi Delta Lake yang kompatibel dengan Spark Anda
    delta_version = "3.1.0" # Contoh: "2.4.0", "3.0.0", "3.1.0"
    # Scala version (biasanya 2.12 untuk Spark 3.x)
    scala_version = "2.12"


    # Jar packages
    # Perhatikan: untuk Spark 3.5+, Delta Lake seringkali dibundle atau menggunakan `delta-spark`
    # Jika menggunakan Spark 3.5+ dengan Delta Lake 3.x, Anda mungkin hanya perlu `io.delta:delta-spark_2.12:3.1.0`
    # Untuk Spark < 3.5, `delta-core` lebih umum. Cek dokumentasi Delta Lake.
    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version_short}.0", # Sesuaikan versi Spark penuh jika perlu
        f"io.delta:delta-spark_{scala_version}:{delta_version}" # Atau delta-core
        # Tambahkan paket lain jika perlu, misal untuk konektor JDBC, dll.
        # "org.apache.hadoop:hadoop-aws:3.3.4" # Kadang diperlukan untuk S3A, tergantung distribusi Spark
    ]

    spark_builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        # Konfigurasi untuk Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Konfigurasi untuk akses MinIO (S3 compatible storage)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true") # Penting untuk MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Opsional: Optimasi untuk Delta Lake
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # Opsional: Untuk mengatasi masalah timestamp dengan Parquet di beberapa environment
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    )

    # Untuk development lokal, Anda bisa menambahkan .master("local[*]")
    # Di cluster, master URL akan diset oleh spark-submit atau environment
    # spark_builder = spark_builder.master("local[*]") # Uncomment untuk dev lokal

    try:
        spark = spark_builder.getOrCreate()
        print(f"SparkSession '{app_name}' initialized successfully.")
        print(f"Spark version: {spark.version}")
        # Atur log level untuk mengurangi verbosity, bisa juga diatur di log4j.properties
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        print(f"Error initializing SparkSession: {e}")
        raise

if __name__ == "__main__":
    # Contoh penggunaan
    spark_session = get_spark_session("TestSession")
    if spark_session:
        print("Spark session created. You can now use it.")
        data = [("Alice", 1)]
        df = spark_session.createDataFrame(data, ["Name", "ID"])
        df.show()
        spark_session.stop()