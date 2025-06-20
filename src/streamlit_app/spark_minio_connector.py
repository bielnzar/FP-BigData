import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
import os
import logging

# Setup logger sederhana
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# Konfigurasi MinIO (Ambil dari environment variables jika didefinisikan di docker-compose untuk streamlit-app)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "bosmuda")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "Kelompok6")
MINIO_BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME", "fp-bigdata")

GOLD_BASE_PATH_S3A = f"s3a://{MINIO_BUCKET_NAME}/gold"
GOLD_COUNTRY_YEARLY_SUMMARY_S3A_PATH = f"{GOLD_BASE_PATH_S3A}/country_yearly_health_summary"
GOLD_ML_FEATURES_S3A_PATH = f"{GOLD_BASE_PATH_S3A}/ml_features_health_disparity"

# Variabel global untuk SparkSession agar tidak diinisialisasi ulang terus-menerus
_spark_session = None

# Menggunakan st.cache_resource untuk SparkSession
@st.cache_resource
def get_spark_session():
    """
    Membuat atau mengembalikan SparkSession yang sudah ada.
    JARs untuk Delta dan S3A (hadoop-aws) harus sudah ada di classpath.
    Ini bisa dicapai dengan menyediakannya saat menjalankan Streamlit (jika di host)
    atau dengan membangun image Docker Streamlit yang sudah menyertakan Spark dan JARs.
    """
    global _spark_session
    if _spark_session is None:
        logger.info("Initializing SparkSession for Streamlit app...")
        try:
            # Konfigurasi ini mengasumsikan JARs (Delta, Kafka, Hadoop-AWS)
            # akan tersedia di lingkungan tempat Streamlit berjalan.
            # Jika Streamlit berjalan di kontainer Python biasa, Anda perlu menambahkan
            # spark.jars.packages atau memastikan JARs ada di path.
            # Namun, jika image Docker Streamlit Anda dibangun dari image Spark atau
            # menyertakan Spark dan JARs, ini akan bekerja.
            #
            # Mengingat Dockerfile.streamlit Anda menggunakan python:3.9-slim,
            # kita perlu menambahkan konfigurasi packages di sini, atau lebih baik,
            # sertakan JARs di Dockerfile.streamlit (mirip Dockerfile.spark).
            # Untuk kesederhanaan saat ini, kita coba dengan packages.
            
            # Versi harus SAMA dengan yang ada di Dockerfile.spark Anda
            # Ini akan diunduh oleh Spark saat sesi dibuat jika tidak ada.
            DELTA_VERSION_FOR_PACKAGES = "2.4.0" # Sesuai dengan Dockerfile.spark
            HADOOP_AWS_VERSION_FOR_PACKAGES = "3.3.4" # Sesuai dengan Dockerfile.spark
            AWS_SDK_BUNDLE_VERSION_FOR_PACKAGES = "1.12.262" # Sesuai dengan Dockerfile.spark


            _spark_session = SparkSession.builder \
                .appName("StreamlitMinIOReader") \
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
                .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.warehouse.dir", f"s3a://{MINIO_BUCKET_NAME}/_spark_warehouse_streamlit") \
                .config("spark.ui.enabled", "false") \
                .config("spark.jars.packages", 
                        f"io.delta:delta-core_2.12:{DELTA_VERSION_FOR_PACKAGES}," + \
                        f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION_FOR_PACKAGES}," + \
                        f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_BUNDLE_VERSION_FOR_PACKAGES}") \
                .getOrCreate()
            _spark_session.sparkContext.setLogLevel("WARN")
            logger.info("SparkSession initialized successfully for Streamlit.")
        except Exception as e:
            logger.error(f"Failed to initialize SparkSession: {e}", exc_info=True)
            st.error(f"Gagal menginisialisasi Spark: {e}")
            _spark_session = None # Pastikan None jika gagal
    return _spark_session

@st.cache_data(ttl=300) # Cache data DataFrame selama 5 menit
def load_delta_table_as_pandas(delta_s3a_path: str):
    """Membaca tabel Delta dari MinIO S3A dan mengembalikannya sebagai DataFrame Pandas."""
    spark = get_spark_session()
    if spark is None:
        logger.error(f"Spark session not available. Cannot load data from {delta_s3a_path}")
        return pd.DataFrame()
    
    try:
        logger.info(f"Reading Delta table from: {delta_s3a_path}")
        spark_df = spark.read.format("delta").load(delta_s3a_path)
        pandas_df = spark_df.toPandas() # Konversi ke Pandas DataFrame
        logger.info(f"Successfully loaded {len(pandas_df)} rows from {delta_s3a_path} into Pandas DataFrame.")
        return pandas_df
    except Exception as e:
        logger.error(f"Failed to load Delta table '{delta_s3a_path}': {e}", exc_info=True)
        st.error(f"Gagal memuat data dari '{delta_s3a_path.split('/')[-1]}': {e}")
        return pd.DataFrame()