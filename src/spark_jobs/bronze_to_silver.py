from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, monotonically_increasing_id, sha2, concat_ws, regexp_replace
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType
)
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT_URL_DOCKER = os.environ.get("MINIO_ENDPOINT_URL_DOCKER", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

def get_spark_session():
    return (
        SparkSession.builder.appName("BronzeToSilverProcessing")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_URL_DOCKER)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

health_stats_schema = StructType([
    StructField("Country", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Disease_Name", StringType(), True),
    StructField("Disease_Category", StringType(), True),
    StructField("Prevalence_Rate_Percent", FloatType(), True),
    StructField("Incidence_Rate_Percent", FloatType(), True),
    StructField("Mortality_Rate_Percent", FloatType(), True),
    StructField("Age_Group", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Population_Affected", IntegerType(), True),
    StructField("Healthcare_Access_Percent", FloatType(), True),
    StructField("Doctors_per_1000", FloatType(), True),
    StructField("Hospital_Beds_per_1000", FloatType(), True),
    StructField("Treatment_Type", StringType(), True),
    StructField("Average_Treatment_Cost_USD", IntegerType(), True),
    StructField("Availability_of_Vaccines_Treatment", StringType(), True),
    StructField("Recovery_Rate_Percent", FloatType(), True),
    StructField("DALYs", FloatType(), True),
    StructField("Improvement_in_5_Years_Percent", FloatType(), True),
    StructField("Per_Capita_Income_USD", IntegerType(), True),
    StructField("Education_Index", FloatType(), True), 
    StructField("Urbanization_Rate_Percent", FloatType(), True)
])

medical_abstracts_schema = StructType([
    StructField("text", StringType(), True),
    StructField("label", StringType(), True)
])


def rename_columns_for_parquet(df):
    """Mengganti karakter yang tidak valid untuk Parquet/Delta di nama kolom."""
    new_columns = []
    for c in df.columns:
        new_c = c.replace(" (%)", "_Percent") \
                 .replace(" ", "_") \
                 .replace("(", "") \
                 .replace(")", "") \
                 .replace("/", "_") \
                 .replace("-", "_")
        new_columns.append(new_c)
    return df.toDF(*new_columns)


def process_health_statistics(spark):
    """Memproses data Global Health Statistics dari Bronze ke Silver."""
    bronze_path = "s3a://bronze/global-health-stats/"
    silver_path = "s3a://silver/global-health-statistics"
    
    print(f"Reading Global Health Statistics from Bronze: {bronze_path}")
    df_bronze_raw = spark.read.format("json").load(bronze_path)

    if df_bronze_raw.isEmpty():
        print("No data found in Bronze for Global Health Statistics. Exiting.")
        return

    df_bronze_renamed = rename_columns_for_parquet(df_bronze_raw)
    
    df_bronze_casted = df_bronze_renamed
    for field in health_stats_schema.fields:
        df_bronze_casted = df_bronze_casted.withColumn(field.name, col(field.name).cast(field.dataType))
    
    print("Schema after casting for Health Statistics:")
    df_bronze_casted.printSchema()

    print("DEBUG: Mencari nilai 'Age_Group' yang bermasalah SEBELUM dibersihkan...")
    df_bronze_casted.select("Age_Group").filter(col("Age_Group").contains("s")).show(5, truncate=False)

    df_cleaned_values = df_bronze_casted.withColumn(
        "Age_Group",
        regexp_replace(col("Age_Group"), "[^0-9\\-+\\s]", "") # Hanya izinkan angka, strip, plus, dan spasi
    )

    print("DEBUG: Memeriksa nilai 'Age_Group' SETELAH dibersihkan (seharusnya tidak ada 's' lagi)...")
    df_cleaned_values.select("Age_Group").filter(col("Age_Group").contains("s")).show(5, truncate=False)

    key_cols_health = ["Country", "Year", "Disease_Name"]
    df_cleaned = df_cleaned_values.na.drop(subset=key_cols_health)
    
    dedup_key_cols_health = [
        "Country", "Year", "Disease_Name", "Age_Group", "Gender", 
        "Prevalence_Rate_Percent", "Incidence_Rate_Percent", "Mortality_Rate_Percent"
    ]
    
    print("Generating hash for deduplication...")
    df_with_hash = df_cleaned.withColumn(
        "record_hash", 
        sha2(concat_ws("||", *[col(c).cast("string") for c in dedup_key_cols_health]), 256)
    )

    df_source_data = df_with_hash.dropDuplicates(subset=["record_hash"])
    
    print(f"Writing data to Silver layer at {silver_path} using Parquet format...")
    df_source_data.write.format("parquet").mode("overwrite").save(silver_path)
    print("Write operation completed.")

    print("Global Health Statistics processing to Silver completed.")
    
    print("Sample of data written to Silver (Health Statistics):")
    spark.read.format("parquet").load(silver_path).show(5, truncate=False, vertical=True)


def process_medical_abstracts(spark):
    """Memproses data Medical Abstracts dari Bronze ke Silver."""
    bronze_path = "s3a://bronze/medical-abstracts/"
    silver_path = "s3a://silver/medical-abstracts"

    print(f"Reading Medical Abstracts from Bronze: {bronze_path}")
    df_bronze_raw = spark.read.format("json").schema(medical_abstracts_schema).load(bronze_path)

    if df_bronze_raw.isEmpty():
        print("No data found in Bronze for Medical Abstracts. Exiting.")
        return

    print("Schema for Medical Abstracts from Bronze:")
    df_bronze_raw.printSchema()

    df_cleaned = df_bronze_raw.withColumn("text", trim(col("text"))) \
                              .withColumn("label", trim(lower(col("label")))) # Label juga di lower case

    df_cleaned = df_cleaned.filter((col("text") != "") & col("text").isNotNull() & \
                                   (col("label") != "") & col("label").isNotNull())

    dedup_key_cols_abstracts = ["text", "label"]
    
    print("Generating hash for deduplication...")
    df_with_hash_abstracts = df_cleaned.withColumn(
        "record_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in dedup_key_cols_abstracts]), 256)
    )
    df_source_data = df_with_hash_abstracts.dropDuplicates(subset=["record_hash"])
    
    print(f"Writing data to Silver layer at {silver_path} using Parquet format...")
    df_source_data.write.format("parquet").mode("overwrite").save(silver_path)
    print("Write operation completed.")

    print("Medical Abstracts processing to Silver completed.")

    print("Sample of data written to Silver (Medical Abstracts):")
    spark.read.format("parquet").load(silver_path).show(5, truncate=False, vertical=True)


if __name__ == "__main__":
    spark_session = get_spark_session()
    
    print("Starting Bronze to Silver processing for Global Health Statistics...")
    process_health_statistics(spark_session)
    
    print("\nStarting Bronze to Silver processing for Medical Abstracts...")
    process_medical_abstracts(spark_session)
    
    spark_session.stop()
    print("Spark session stopped.")