from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, lower, regexp_replace, split, udf
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, BooleanType, ArrayType

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "bosmuda"
MINIO_SECRET_KEY = "Kelompok6"
MINIO_BUCKET_NAME = "fp-bigdata"

BRONZE_BASE_PATH = f"s3a://{MINIO_BUCKET_NAME}/bronze"
SILVER_BASE_PATH = f"s3a://{MINIO_BUCKET_NAME}/silver"

BRONZE_STRUCTURED_JSONL_PATH = f"{BRONZE_BASE_PATH}/structured_data/*/*/*/*.jsonl"
BRONZE_UNSTRUCTURED_JSONL_PATH = f"{BRONZE_BASE_PATH}/unstructured_data/*/*/*/*.jsonl"

# Path output ke Silver (akan menjadi tabel Delta)
SILVER_STRUCTURED_PATH = f"{SILVER_BASE_PATH}/structured_health_data_cleaned"
SILVER_UNSTRUCTURED_PATH = f"{SILVER_BASE_PATH}/unstructured_text_processed"


def create_spark_session_for_batch():
    """Membuat SparkSession untuk job batch dengan konfigurasi MinIO dan Delta."""
    print("INFO: Creating Spark session for batch ETL (Bronze to Silver)...")
    spark = SparkSession.builder \
        .appName("BronzeToSilverETL") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("INFO: Spark session created successfully.")
    return spark

# Fungsi Transformasi untuk Data Terstruktur
def transform_structured_data(df_bronze_structured):
    """Membersihkan dan mentransformasi DataFrame data terstruktur."""
    print("INFO: Transforming structured data...")
    df_transformed = df_bronze_structured
    
    # Casting tipe data
    df_transformed = df_transformed \
        .withColumn("Year", col("Year").cast(IntegerType())) \
        .withColumn("Prevalence_Rate_Percent", col("Prevalence_Rate_Percent").cast(DoubleType())) \
        .withColumn("Incidence_Rate_Percent", col("Incidence_Rate_Percent").cast(DoubleType())) \
        .withColumn("Mortality_Rate_Percent", col("Mortality_Rate_Percent").cast(DoubleType())) \
        .withColumn("Population_Affected", col("Population_Affected").cast(IntegerType())) \
        .withColumn("Healthcare_Access_Percent", col("Healthcare_Access_Percent").cast(DoubleType())) \
        .withColumn("Doctors_per_1000", col("Doctors_per_1000").cast(DoubleType())) \
        .withColumn("Hospital_Beds_per_1000", col("Hospital_Beds_per_1000").cast(DoubleType())) \
        .withColumn("Average_Treatment_Cost_USD", col("Average_Treatment_Cost_USD").cast(DoubleType())) \
        .withColumn("Recovery_Rate_Percent", col("Recovery_Rate_Percent").cast(DoubleType())) \
        .withColumn("DALYs", col("DALYs").cast(IntegerType())) \
        .withColumn("Improvement_in_5_Years_Percent", col("Improvement_in_5_Years_Percent").cast(DoubleType())) \
        .withColumn("Per_Capita_Income_USD", col("Per_Capita_Income_USD").cast(DoubleType())) \
        .withColumn("Education_Index", col("Education_Index").cast(DoubleType())) \
        .withColumn("Urbanization_Rate_Percent", col("Urbanization_Rate_Percent").cast(DoubleType()))

    # Konversi "Yes"/"No" ke Boolean
    df_transformed = df_transformed.withColumn(
        "Availability_of_Vaccines_Treatment",
        lower(col("Availability_of_Vaccines_Treatment")) == "yes"
    )

    # trim whitespace
    string_columns = [f.name for f in df_transformed.schema.fields if isinstance(f.dataType, StringType)]
    for str_col in string_columns:
        df_transformed = df_transformed.withColumn(str_col, trim(col(str_col)))

    # Tambahkan kolom waktu proses Spark
    df_transformed = df_transformed.withColumn("silver_processed_timestamp", current_timestamp())
    
    print("INFO: Structured data transformation complete.")
    return df_transformed

# Fungsi Transformasi untuk Data Tidak Terstruktur (NLP)
# stop_words_global = set(stopwords.words('english'))
# stemmer_global = PorterStemmer()

# @udf(ArrayType(StringType()))
# def preprocess_text_udf(text):
#     if text is None:
#         return []
#     text = lower(text) # Sudah dilakukan oleh fungsi Spark
#     text = regexp_replace(text, r'[^a-z\s]', '') # Sudah dilakukan oleh fungsi Spark
#     tokens = nltk.word_tokenize(text)
#     processed_tokens = [stemmer_global.stem(token) for token in tokens if token not in stop_words_global and len(token) > 1]
#     return processed_tokens

def transform_unstructured_data(df_bronze_unstructured):
    """Membersihkan dan melakukan pra-pemrosesan NLP pada DataFrame data tidak terstruktur."""
    print("INFO: Transforming unstructured data (NLP)...")
    df_transformed = df_bronze_unstructured

    # Pembersihan teks dasar menggunakan fungsi Spark SQL
    df_transformed = df_transformed.withColumn("cleaned_text", lower(col("text"))) # Kolom 'text' dari JSONL Anda
    df_transformed = df_transformed.withColumn("cleaned_text", regexp_replace(col("cleaned_text"), r'[^a-z0-9\s]', "")) # Hapus non-alphanumeric kecuali spasi
    df_transformed = df_transformed.withColumn("cleaned_text", regexp_replace(col("cleaned_text"), r'\s+', " ")) # Ganti multiple spasi dgn satu
    df_transformed = df_transformed.withColumn("cleaned_text", trim(col("cleaned_text")))

    # Tokenisasi (split berdasarkan spasi)
    df_transformed = df_transformed.withColumn("tokens", split(col("cleaned_text"), " "))
    
    # Penghapusan Stopwords
    # stop_words = ['is', 'a', 'the', 'and', 'in', 'it', 'to', 'of', 'for', 'on', 'with', 'this', 'that']
    # df_transformed = df_transformed.withColumn("filtered_tokens", expr(f"filter(tokens, x -> not(array_contains(array{stop_words}, x)))"))
    # Atau menggunakan StopWordsRemover dari Spark MLlib (lebih efisien)
    from pyspark.ml.feature import StopWordsRemover
    remover = StopWordsRemover(inputCol="tokens", outputCol="filtered_tokens")
    df_transformed = remover.transform(df_transformed)


    # Stemming/Lemmatization - ini lebih kompleks dan sering membutuhkan UDF dengan NLTK/SpaCy
    # Jika menggunakan UDF, pastikan library terinstal di worker.
    # df_transformed = df_transformed.withColumn("stemmed_tokens", preprocess_text_udf(col("text")))

    # Tambahkan kolom waktu proses Spark
    df_transformed = df_transformed.withColumn("silver_processed_timestamp", current_timestamp())

    print("INFO: Unstructured data transformation complete.")
    return df_transformed


if __name__ == "__main__":
    spark = create_spark_session_for_batch()

    # --- Proses Data Terstruktur ---
    try:
        print(f"INFO: Reading structured data from Bronze: {BRONZE_STRUCTURED_JSONL_PATH}")
        bronze_structured_df = spark.read.format("json").load(BRONZE_STRUCTURED_JSONL_PATH)
        
        print("INFO: Schema of bronze_structured_df after reading from JSONL:")
        bronze_structured_df.printSchema()
        print(f"INFO: Count of rows in bronze_structured_df: {bronze_structured_df.count()}")
        bronze_structured_df.show(5, truncate=False, vertical=True)

        if bronze_structured_df.count() > 0:
            silver_structured_df = transform_structured_data(bronze_structured_df)
            
            print("INFO: Schema of silver_structured_df before writing to Delta:")
            silver_structured_df.printSchema()
            silver_structured_df.show(5, truncate=False, vertical=True)

            print(f"INFO: Writing cleaned structured data to Silver (Delta Lake): {SILVER_STRUCTURED_PATH}")
            silver_structured_df.write.format("delta").mode("overwrite").save(SILVER_STRUCTURED_PATH)
            print("INFO: Successfully wrote structured data to Silver.")
        else:
            print("INFO: No structured data found in Bronze to process.")

    except Exception as e:
        print(f"ERROR: Failed to process structured data: {e}")
        import traceback
        traceback.print_exc()

    try:
        print(f"INFO: Reading unstructured data from Bronze: {BRONZE_UNSTRUCTURED_JSONL_PATH}")
        bronze_unstructured_df = spark.read.format("json").load(BRONZE_UNSTRUCTURED_JSONL_PATH)

        print("INFO: Schema of bronze_unstructured_df after reading from JSONL:")
        bronze_unstructured_df.printSchema()
        print(f"INFO: Count of rows in bronze_unstructured_df: {bronze_unstructured_df.count()}")
        bronze_unstructured_df.show(5, truncate=False, vertical=True)
        
        if bronze_unstructured_df.count() > 0:
            silver_unstructured_df = transform_unstructured_data(bronze_unstructured_df)

            print("INFO: Schema of silver_unstructured_df before writing to Delta:")
            silver_unstructured_df.printSchema()
            silver_unstructured_df.show(5, truncate=False, vertical=True)
            
            # Pilih kolom yang relevan untuk disimpan di Silver (tidak perlu `cleaned_text` jika sudah ada `tokens` dan `filtered_tokens`)
            columns_to_keep_unstructured = ["text", "label", "source_document", "tokens", "filtered_tokens", "silver_processed_timestamp"]
            final_silver_unstructured_df = silver_unstructured_df.select([col_name for col_name in columns_to_keep_unstructured if col_name in silver_unstructured_df.columns])


            print(f"INFO: Writing processed unstructured data to Silver (Delta Lake): {SILVER_UNSTRUCTURED_PATH}")
            final_silver_unstructured_df.write.format("delta").mode("overwrite").save(SILVER_UNSTRUCTURED_PATH)
            print("INFO: Successfully wrote unstructured data to Silver.")
        else:
            print("INFO: No unstructured data found in Bronze to process.")

    except Exception as e:
        print(f"ERROR: Failed to process unstructured data: {e}")
        import traceback
        traceback.print_exc()

    print("INFO: ETL Bronze to Silver job finished.")
    spark.stop()