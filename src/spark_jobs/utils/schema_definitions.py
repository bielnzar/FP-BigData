# src/spark_jobs/utils/schema_definitions.py
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    TimestampType # Kita akan menambahkan timestamp ingest di Spark
)

# Skema untuk data Global Health Statistics dari Kafka
# Semua kolom dari CSV akan dibaca sebagai String dari Kafka JSON value,
# casting ke tipe yang benar akan dilakukan di Spark setelah parsing JSON.
# Atau, jika publisher mengirim dengan tipe data yang benar dalam JSON, Spark bisa infer.
# Untuk kontrol lebih, definisikan skema yang diharapkan setelah parsing JSON.
# Di sini kita definisikan tipe data akhir yang kita inginkan di Bronze.

# Kolom dari Global Health Statistics.csv:
# Country,Year,Disease Name,Disease Category,Prevalence Rate (%),Incidence Rate (%),
# Mortality Rate (%),Age Group,Gender,Population Affected,Healthcare Access (%),
# Doctors per 1000,Hospital Beds per 1000,Treatment Type,Average Treatment Cost (USD),
# Availability of Vaccines/Treatment,Recovery Rate (%),DALYs,Improvement in 5 Years (%),
# Per Capita Income (USD),Education Index,Urbanization Rate (%)

structured_health_stats_schema = StructType([
    StructField("Country", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Disease_Name", StringType(), True), # Mengganti "Disease Name"
    StructField("Disease_Category", StringType(), True), # Mengganti "Disease Category"
    StructField("Prevalence_Rate_Percent", DoubleType(), True), # Mengganti "Prevalence Rate (%)"
    StructField("Incidence_Rate_Percent", DoubleType(), True), # Mengganti "Incidence Rate (%)"
    StructField("Mortality_Rate_Percent", DoubleType(), True), # Mengganti "Mortality Rate (%)"
    StructField("Age_Group", StringType(), True), # Mengganti "Age Group"
    StructField("Gender", StringType(), True),
    StructField("Population_Affected", IntegerType(), True), # Mengganti "Population Affected"
    StructField("Healthcare_Access_Percent", DoubleType(), True), # Mengganti "Healthcare Access (%)"
    StructField("Doctors_per_1000", DoubleType(), True), # Mengganti "Doctors per 1000"
    StructField("Hospital_Beds_per_1000", DoubleType(), True), # Mengganti "Hospital Beds per 1000"
    StructField("Treatment_Type", StringType(), True), # Mengganti "Treatment Type"
    StructField("Average_Treatment_Cost_USD", IntegerType(), True), # Mengganti "Average Treatment Cost (USD)"
    StructField("Availability_of_Vaccines_Treatment", StringType(), True), # Mengganti "Availability of Vaccines/Treatment" (Bisa BooleanType jika nilainya "Yes"/"No" konsisten)
    StructField("Recovery_Rate_Percent", DoubleType(), True), # Mengganti "Recovery Rate (%)"
    StructField("DALYs", IntegerType(), True), # Asumsi integer, bisa DoubleType
    StructField("Improvement_in_5_Years_Percent", DoubleType(), True), # Mengganti "Improvement in 5 Years (%)"
    StructField("Per_Capita_Income_USD", IntegerType(), True), # Mengganti "Per Capita Income (USD)"
    StructField("Education_Index", DoubleType(), True), # Mengganti "Education Index"
    StructField("Urbanization_Rate_Percent", DoubleType(), True), # Mengganti "Urbanization Rate (%)"
    StructField("ingest_timestamp", TimestampType(), False) # Akan ditambahkan oleh Spark
])

# Skema untuk data medical_text_classification_fake_dataset.csv dari Kafka
# Kolom: text,label
unstructured_text_schema = StructType([
    StructField("text", StringType(), True),    # Kolom utama berisi teks
    StructField("label", StringType(), True),   # Kategori/label
    # Opsional: tambahkan field yang mungkin dikirim oleh publisher tidak terstruktur
    StructField("source_document", StringType(), True),
    StructField("ingest_timestamp", TimestampType(), False) # Akan ditambahkan oleh Spark
])