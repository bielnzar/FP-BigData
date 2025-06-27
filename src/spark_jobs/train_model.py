from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import os

def get_spark_session():
    """Mengkonfigurasi dan mengembalikan SparkSession."""
    MINIO_ENDPOINT_URL_DOCKER = os.environ.get("MINIO_ENDPOINT_URL_DOCKER", "http://minio:9000")
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    return (
        SparkSession.builder.appName("TrainMortalityPredictionModel")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_URL_DOCKER)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    silver_health_stats_path = "s3a://silver/global-health-statistics"
    model_save_path = "s3a://models/mortality-rate-predictor"
    
    print(f"Loading data from Silver layer: {silver_health_stats_path}")
    try:
        df_silver = spark.read.format("parquet").load(silver_health_stats_path)
    except Exception as e:
        print(f"Error loading data from Silver: {e}. Stopping job.")
        spark.stop()
        return

    # Mengurangi fitur untuk fokus pada yang paling relevan, termasuk fitur ekonomi kunci
    feature_cols = [
        "Year", "Prevalence_Rate_Percent", "Incidence_Rate_Percent",
        "Average_Treatment_Cost_USD", "Recovery_Rate_Percent", "DALYs",
        "Per_Capita_Income_USD",  # Menambahkan kembali fitur penting dari EDA
        # Fitur kategorikal inti
        "Country", "Disease_Category", "Age_Group", "Gender"
    ]
    target_col = "Mortality_Rate_Percent"

    df_model_data = df_silver.select(feature_cols + [target_col]).na.drop()

    # df_model_data = df_model_data.sample(fraction=0.1, seed=42)

    if df_model_data.isEmpty():
        print("No data available for training after dropping nulls. Stopping job.")
        spark.stop()
        return

    categorical_cols = ["Country", "Disease_Category", "Age_Group", "Gender"]
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in categorical_cols]

    encoder_outputs = [f"{c}_vec" for c in categorical_cols]
    encoder = OneHotEncoder(inputCols=[f"{c}_index" for c in categorical_cols], outputCols=encoder_outputs)

    numeric_cols = [c for c in feature_cols if c not in categorical_cols]
    assembler_inputs = numeric_cols + encoder_outputs
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    rf = RandomForestRegressor(featuresCol="features", labelCol=target_col)

    pipeline = Pipeline(stages=indexers + [encoder, assembler, rf])

    (training_data, test_data) = df_model_data.randomSplit([0.8, 0.2], seed=42)
    
    print("Training the model...")
    model = pipeline.fit(training_data)
    
    print("Making predictions on the test set...")
    predictions = model.transform(test_data)

    print("Sample predictions:")
    predictions.select(target_col, "prediction").show(5)

    evaluator_rmse = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")
    
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    
    print(f"Model Performance on Test Data:")
    print(f"Root Mean Squared Error (RMSE) = {rmse}")
    print(f"R-squared (R2) = {r2}")

    print(f"Saving the trained model to: {model_save_path}")
    model.write().overwrite().save(model_save_path)
    print("Model saved successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
