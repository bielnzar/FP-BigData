from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, countDistinct, min, max
import os

MINIO_ENDPOINT_URL_DOCKER = os.environ.get("MINIO_ENDPOINT_URL_DOCKER", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

def get_spark_session():
    """Mengkonfigurasi dan mengembalikan SparkSession dengan dukungan Hive Metastore."""
    return (
        SparkSession.builder.appName("SilverToGoldProcessing")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_URL_DOCKER)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "/opt/bitnami/spark/spark-warehouse")
        .config("hive.metastore.warehouse.dir", "/opt/bitnami/spark/spark-warehouse")
        .config("hive.metastore.schema.verification", "false")
        .config("datanucleus.schema.autoCreateAll", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

def create_country_health_summary_gold(spark, df_silver, gold_path_target):
    """
    Membuat tabel ringkasan per negara.
    Tujuan: Untuk dashboard ringkasan tingkat tinggi per negara.
    """
    print("Creating country_health_summary_gold...")
    df_gold = df_silver.groupBy("Country").agg(
        avg("Mortality_Rate_Percent").alias("Avg_Mortality_Rate_Percent"),
        avg("DALYs").alias("Avg_DALYs"),
        avg("Prevalence_Rate_Percent").alias("Avg_Prevalence_Rate_Percent"),
        avg("Incidence_Rate_Percent").alias("Avg_Incidence_Rate_Percent"),
        sum("Population_Affected").alias("Total_Population_Affected_Overall"),
        avg("Healthcare_Access_Percent").alias("Avg_Healthcare_Access_Percent"),
        avg("Doctors_per_1000").alias("Avg_Doctors_per_1000"),
        avg("Hospital_Beds_per_1000").alias("Avg_Hospital_Beds_per_1000"),
        avg("Per_Capita_Income_USD").alias("Avg_Per_Capita_Income_USD"),
        avg("Education_Index").alias("Avg_Education_Index"),
        avg("Urbanization_Rate_Percent").alias("Avg_Urbanization_Rate_Percent"),
        countDistinct("Year").alias("Num_Years_Data"),
        min("Year").alias("First_Year_Data"),
        max("Year").alias("Last_Year_Data")
    )

    print(f"Writing country_health_summary_gold to: {gold_path_target}")
    df_gold.write.format("parquet").mode("overwrite").option("path", gold_path_target).saveAsTable("country_health_summary")
    print("country_health_summary_gold table created.")
    df_gold.show(5, truncate=False, vertical=True)

def create_yearly_disease_category_summary_gold(spark, df_silver, gold_path_target):
    """
    Membuat tabel ringkasan per tahun dan kategori penyakit.
    Tujuan: Untuk menganalisis tren penyakit secara global dari waktu ke waktu.
    """
    print("Creating yearly_disease_category_summary_gold...")
    df_gold = df_silver.groupBy("Year", "Disease_Category").agg(
        avg("Mortality_Rate_Percent").alias("Global_Avg_Mortality_Rate_Percent"),
        avg("DALYs").alias("Global_Avg_DALYs"),
        sum("Population_Affected").alias("Global_Total_Population_Affected"),
        countDistinct("Country").alias("Num_Countries_Reported")
    ).orderBy("Year", "Disease_Category")

    print(f"Writing yearly_disease_category_summary_gold to: {gold_path_target}")
    df_gold.write.format("parquet").mode("overwrite").option("path", gold_path_target).saveAsTable("yearly_disease_category_summary")
    print("yearly_disease_category_summary_gold table created.")
    df_gold.show(5, truncate=False, vertical=True)

def create_country_disease_yearly_details_gold(spark, df_silver, gold_path_target):
    """
    Membuat tabel detail (denormalisasi) untuk analisis mendalam.
    Tujuan: Menyediakan data granular untuk drill-down di dashboard.
    """
    print("Creating country_disease_yearly_details_gold...")
    df_gold = df_silver.select(
        "Country",
        "Year",
        "Disease_Name",
        "Disease_Category",
        "Mortality_Rate_Percent",
        "DALYs",
        "Prevalence_Rate_Percent",
        "Incidence_Rate_Percent",
        "Population_Affected",
        col("Healthcare_Access_Percent").alias("Healthcare_Access_Percent_CountryYear"),
        col("Per_Capita_Income_USD").alias("Per_Capita_Income_USD_CountryYear"),
        col("Education_Index").alias("Education_Index_CountryYear")
    )

    print(f"Writing country_disease_yearly_details_gold to: {gold_path_target}")
    df_gold.write.format("parquet").mode("overwrite").option("path", gold_path_target).saveAsTable("country_disease_yearly_details")
    print("country_disease_yearly_details_gold table created.")
    df_gold.show(5, truncate=False, vertical=True)

def create_disease_abstract_counts_gold(spark, df_silver_abstracts, gold_path_target):
    """
    Membuat tabel hitungan abstrak per kategori penyakit.
    Tujuan: Mengukur volume penelitian untuk setiap kategori penyakit.
    """
    print("Creating disease_abstract_counts_gold...")
    df_gold = df_silver_abstracts.groupBy(col("label").alias("Disease_Category")).agg(
        countDistinct("record_hash").alias("Abstract_Count")
    ).orderBy(col("Abstract_Count").desc())

    print(f"Writing disease_abstract_counts_gold to: {gold_path_target}")
    df_gold.write.format("parquet").mode("overwrite").option("path", gold_path_target).saveAsTable("gold.disease_abstract_counts")
    print("disease_abstract_counts_gold table created.")
    df_gold.show(5, truncate=False)

def create_combined_analysis_gold(spark, df_silver_health, df_abstract_counts, gold_path_target):
    """
    (BARU) Menggabungkan data kesehatan dengan data abstrak untuk analisis mendalam.
    Tujuan: Menemukan wawasan dengan membandingkan dampak penyakit vs. volume penelitian.
    """
    print("Creating combined disease_impact_vs_research_gold...")

    df_health_summary = df_silver_health.groupBy("Disease_Category").agg(
        sum("Population_Affected").alias("Total_Population_Affected"),
        avg("Mortality_Rate_Percent").alias("Avg_Mortality_Rate")
    )

    df_gold = df_health_summary.join(
        df_abstract_counts,
        df_health_summary.Disease_Category == df_abstract_counts.Disease_Category,
        "left"
    ).select(
        df_health_summary.Disease_Category,
        col("Total_Population_Affected"),
        col("Avg_Mortality_Rate"),
        col("Abstract_Count")
    ).orderBy(col("Total_Population_Affected").desc())

    print(f"Writing disease_impact_vs_research_gold to: {gold_path_target}")
    df_gold.write.format("parquet").mode("overwrite").option("path", gold_path_target).saveAsTable("gold.disease_impact_vs_research")
    print("disease_impact_vs_research_gold table created.")
    df_gold.show(10, truncate=False)


if __name__ == "__main__":
    spark = get_spark_session()

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        print("Gold database created or already exists.")
    except Exception as e:
        print(f"Warning: Could not create gold database via Hive Metastore: {e}")
        print("Continuing with table creation without explicit database creation...")

    try:
        spark.sql("USE gold")
        print("Using 'gold' schema.")
    except Exception as e:
        print(f"Error: Schema 'gold' not found or cannot be used: {e}")
        spark.stop()
        exit()

    silver_health_stats_path = "s3a://silver/global-health-statistics"
    silver_medical_abstracts_path = "s3a://silver/medical-abstracts"
    
    gold_country_summary_path = "s3a://gold/country-health-summary"
    gold_yearly_disease_cat_path = "s3a://gold/yearly-disease-category-summary"
    gold_country_disease_yearly_path = "s3a://gold/country-disease-yearly-details"
    gold_abstract_counts_path = "s3a://gold/disease-abstract-counts"
    gold_combined_analysis_path = "s3a://gold/disease-impact-vs-research"

    print(f"Reading data from Silver layer...")
    try:
        df_silver_health = spark.read.format("parquet").load(silver_health_stats_path)
        df_silver_abstracts = spark.read.format("parquet").load(silver_medical_abstracts_path)

        df_silver_health.cache()
        df_silver_abstracts.cache()
    except Exception as e:
        print(f"Error reading from Silver layer: {e}. Stopping job.")
        spark.stop()
        exit()

    if not df_silver_health.isEmpty():
        print("\n--- Processing Health Statistics Gold Tables ---")
        create_country_health_summary_gold(spark, df_silver_health, gold_country_summary_path)
        create_yearly_disease_category_summary_gold(spark, df_silver_health, gold_yearly_disease_cat_path)
        create_country_disease_yearly_details_gold(spark, df_silver_health, gold_country_disease_yearly_path)
    else:
        print("\nSkipping Health Statistics Gold processing, Silver data is empty.")

    if not df_silver_abstracts.isEmpty():
        print("\n--- Processing Medical Abstracts Gold Table ---")
        create_disease_abstract_counts_gold(spark, df_silver_abstracts, gold_abstract_counts_path)
    
        df_abstract_counts_gold = spark.read.format("parquet").load(gold_abstract_counts_path)

        if not df_silver_health.isEmpty():
            print("\n--- Processing Combined Analysis Gold Table ---")
            create_combined_analysis_gold(spark, df_silver_health, df_abstract_counts_gold, gold_combined_analysis_path)
        else:
            print("\nSkipping Combined Analysis Gold processing, Silver health data is empty.")
    else:
        print("\nSkipping Abstracts and Combined Gold processing, Silver abstracts data is empty.")

    print("\nAll Silver to Gold processing completed.")
    spark.stop()
    print("Spark session stopped.")