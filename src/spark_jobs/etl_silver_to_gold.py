from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, first, expr, year, udf, lower, trim
from pyspark.sql.types import StringType, MapType # Untuk UDF jika diperlukan nanti

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "bosmuda"
MINIO_SECRET_KEY = "Kelompok6"
MINIO_BUCKET_NAME = "fp-bigdata"

SILVER_BASE_PATH = f"s3a://{MINIO_BUCKET_NAME}/silver"
GOLD_BASE_PATH = f"s3a://{MINIO_BUCKET_NAME}/gold"

SILVER_STRUCTURED_PATH = f"{SILVER_BASE_PATH}/structured_health_data_cleaned"
SILVER_UNSTRUCTURED_PATH = f"{SILVER_BASE_PATH}/unstructured_text_processed"

# Path output ke Gold (akan menjadi tabel Delta)
GOLD_COUNTRY_YEARLY_SUMMARY_PATH = f"{GOLD_BASE_PATH}/country_yearly_health_summary"
GOLD_ML_FEATURES_PATH = f"{GOLD_BASE_PATH}/ml_features_health_disparity"
# Tabel untuk analisis teks jika relevan
# GOLD_TEXT_ANALYSIS_SUMMARY_PATH = f"{GOLD_BASE_PATH}/text_analysis_disease_summary"


def create_spark_session_for_gold_etl():
    """Membuat SparkSession untuk job ETL Silver ke Gold."""
    print("INFO: Creating Spark session for Gold ETL...")
    spark = SparkSession.builder \
        .appName("SilverToGoldETL") \
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

def create_country_yearly_summary(spark, silver_structured_df):
    """
    Membuat tabel ringkasan kesehatan per negara per tahun.
    Tujuan: Untuk visualisasi tren dasar di dashboard.
    Fokus pada: Angka kematian, DALYs, prevalensi, insidensi, dan beberapa faktor sosio-ekonomi.
    """
    print("INFO: Creating Country Yearly Health Summary (Gold table)...")
    
    # Pilih kolom yang relevan untuk agregasi dan pastikan tipe datanya sudah benar (seharusnya sudah dari Silver)
    # Kolom numerik yang akan diagregasi (avg atau sum)
    numeric_cols_avg = [
        "Prevalence_Rate_Percent", "Incidence_Rate_Percent", "Mortality_Rate_Percent",
        "Healthcare_Access_Percent", "Doctors_per_1000", "Hospital_Beds_per_1000",
        "Recovery_Rate_Percent", "Improvement_in_5_Years_Percent",
        "Per_Capita_Income_USD", "Education_Index", "Urbanization_Rate_Percent"
    ]
    numeric_cols_sum = ["Population_Affected", "DALYs"]

    agg_expressions = []
    for col_name in numeric_cols_avg:
        agg_expressions.append(avg(col(col_name)).alias(f"avg_{col_name}"))
    for col_name in numeric_cols_sum:
        agg_expressions.append(sum(col(col_name)).alias(f"sum_{col_name}"))
        agg_expressions.append(count(col(col_name)).alias(f"count_records_for_{col_name}")) # Untuk melihat berapa banyak data yang diagregasi

    # Agregasi per Negara dan Tahun
    # Ini akan mengagregasi SEMUA penyakit menjadi satu baris per negara per tahun.
    # Jika Anda ingin per penyakit juga, group by nya perlu ditambah `Disease_Name`.
    # Untuk ringkasan disparitas global, agregat per negara/tahun dulu bisa jadi awal yang baik.
    country_yearly_summary_df = silver_structured_df \
        .groupBy("Country", "Year") \
        .agg(*agg_expressions) \
        .orderBy("Country", "Year")

    print(f"INFO: Schema for Country Yearly Summary (Gold):")
    country_yearly_summary_df.printSchema()
    country_yearly_summary_df.show(10, truncate=False, vertical=True)

    print(f"INFO: Writing Country Yearly Summary to Gold (Delta Lake): {GOLD_COUNTRY_YEARLY_SUMMARY_PATH}")
    country_yearly_summary_df.write.format("delta").mode("overwrite").save(GOLD_COUNTRY_YEARLY_SUMMARY_PATH)
    print("INFO: Successfully wrote Country Yearly Summary to Gold.")
    return country_yearly_summary_df

def create_ml_features_table(spark, silver_structured_df, silver_unstructured_df=None):
    """
    Membuat tabel fitur untuk pelatihan model ML.
    Tujuan: Memprediksi indikator kesehatan kunci (misalnya, Mortality Rate atau DALYs)
             berdasarkan faktor sosio-ekonomi dan infrastruktur kesehatan.
    Ini adalah contoh, Anda perlu mendefinisikan target prediksi (label) dan fitur dengan lebih hati-hati.
    """
    print("INFO: Creating ML Features Table (Gold table)...")

    features_df = silver_structured_df.select(
        "Country",
        "Year",
        "Disease_Name", # Kita mungkin perlu one-hot encode ini atau menggunakan cara lain
        "Disease_Category", # Sama, perlu di-encode
        col("Prevalence_Rate_Percent").alias("feature_prevalence_rate"),
        col("Incidence_Rate_Percent").alias("feature_incidence_rate"),
        col("Population_Affected").alias("feature_population_affected"),
        col("Healthcare_Access_Percent").alias("feature_healthcare_access"),
        col("Doctors_per_1000").alias("feature_doctors_per_1000"),
        col("Hospital_Beds_per_1000").alias("feature_hospital_beds"),
        col("Per_Capita_Income_USD").alias("feature_per_capita_income"),
        col("Education_Index").alias("feature_education_index"),
        col("Urbanization_Rate_Percent").alias("feature_urbanization_rate"),
        # Target prediksi (label)
        col("Mortality_Rate_Percent").alias("label_mortality_rate") # Atau DALYs, dll.
    ).na.drop() # Hapus baris dengan nilai null pada fitur atau label (strategi sederhana)

    # Menggabungkan dengan Fitur dari Data Tidak Terstruktur
    # if silver_unstructured_df is not None:
    #     # Asumsi silver_unstructured_df memiliki 'label' (misal 'Cancer', 'Diabetes') yang bisa
    #     # di-map ke 'Disease_Category' atau 'Disease_Name' di features_df.
    #     # Atau jika Anda melakukan analisis sentimen per negara/tahun dari berita.
    #     # Contoh sederhana: Hitung jumlah artikel per label/kategori penyakit dari data tidak terstruktur
    #     text_summary_df = silver_unstructured_df.groupBy("label") \
    #                         .agg(count("*").alias("num_text_references")) \
    #                         .withColumnRenamed("label", "Disease_Category_from_text")
        
    #     # Join dengan hati-hati (misalnya, Disease_Category)
    #     # features_df = features_df.join(text_summary_df, 
    #     #                                features_df["Disease_Category"] == text_summary_df["Disease_Category_from_text"], 
    #     #                                "left_outer") \
    #     #                            .drop("Disease_Category_from_text") \
    #     #                            .na.fill({"num_text_references": 0})
    #     pass


    print(f"INFO: Schema for ML Features Table (Gold):")
    features_df.printSchema()
    features_df.show(10, truncate=False, vertical=True)

    print(f"INFO: Writing ML Features Table to Gold (Delta Lake): {GOLD_ML_FEATURES_PATH}")
    features_df.write.format("delta").mode("overwrite").save(GOLD_ML_FEATURES_PATH)
    print("INFO: Successfully wrote ML Features Table to Gold.")
    return features_df


if __name__ == "__main__":
    spark = create_spark_session_for_gold_etl()

    # Baca data dari Silver
    print(f"INFO: Reading structured data from Silver: {SILVER_STRUCTURED_PATH}")
    silver_structured_df = spark.read.format("delta").load(SILVER_STRUCTURED_PATH)
    print(f"INFO: Count of rows in silver_structured_df: {silver_structured_df.count()}")
    silver_structured_df.printSchema()

    # Baca data tidak terstruktur dari Silver (opsional untuk tabel Gold awal)
    # print(f"INFO: Reading unstructured data from Silver: {SILVER_UNSTRUCTURED_PATH}")
    # silver_unstructured_df = spark.read.format("delta").load(SILVER_UNSTRUCTURED_PATH)
    # print(f"INFO: Count of rows in silver_unstructured_df: {silver_unstructured_df.count()}")
    # silver_unstructured_df.printSchema()


    # 1. Buat Tabel Ringkasan Kesehatan per Negara per Tahun
    if silver_structured_df.count() > 0:
        country_summary_gold_df = create_country_yearly_summary(spark, silver_structured_df)
    else:
        print("WARN: No structured data in Silver to create country yearly summary.")

    # 2. Buat Tabel Fitur untuk ML
    if silver_structured_df.count() > 0:
        # Untuk contoh ini, kita tidak menggunakan silver_unstructured_df secara langsung di ML features
        ml_features_gold_df = create_ml_features_table(spark, silver_structured_df)
    else:
        print("WARN: No structured data in Silver to create ML features table.")

    print("INFO: ETL Silver to Gold job finished.")
    spark.stop()