from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import os

def get_spark_session():
    """Menginisialisasi dan mengembalikan SparkSession dengan koneksi ke MinIO."""
    MINIO_ENDPOINT_URL_DOCKER = os.environ.get("MINIO_ENDPOINT_URL_DOCKER", "http://minio:9000")
    MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    
    return (
        SparkSession.builder.appName("MortalityPredictionAPI")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_URL_DOCKER)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

app = Flask(__name__)
spark = get_spark_session()
spark.sparkContext.setLogLevel("ERROR")

model_path = "s3a://models/mortality-rate-predictor"
model = None
try:
    model = PipelineModel.load(model_path)
    print(f"Model successfully loaded from {model_path}")
except Exception as e:
    print(f"Warning: Could not load model from {model_path}. Error: {e}")
    print("The /predict endpoint will not work until the model is trained and the service is restarted.")

@app.route('/predict', methods=['POST'])
def predict():
    if not model:
        return jsonify({"error": "Model is not loaded. Please train the model and restart the API service."}), 503 # Service Unavailable

    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid input"}), 400

    try:
        input_df = spark.createDataFrame([data])
        
        prediction_result = model.transform(input_df)

        prediction_value = prediction_result.select("prediction").first()[0]
        
        return jsonify({"mortality_rate_prediction": prediction_value})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint untuk memeriksa status API."""
    status = "OK" if model else "Model not loaded"
    return jsonify({"status": status})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
