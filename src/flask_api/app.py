from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
import os

app = Flask(__name__)

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000") 
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "bosmuda")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "Kelompok6")
MINIO_BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME", "fp-bigdata")

GOLD_BASE_PATH = f"s3a://{MINIO_BUCKET_NAME}/gold"
GOLD_COUNTRY_YEARLY_SUMMARY_PATH = f"{GOLD_BASE_PATH}/country_yearly_health_summary"
GOLD_ML_FEATURES_PATH = f"{GOLD_BASE_PATH}/ml_features_health_disparity" 

spark_session_instance = None

def get_spark_session():
    global spark_session_instance
    if spark_session_instance is None:
        print("INFO: Initializing SparkSession for Flask API...")
        try:
            # spark_session_instance = SparkSession.builder \
            #     .appName("FlaskGoldDataReader") \
            #     .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            #     .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            #     .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            #     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            #     .getOrCreate() # Ini akan membutuhkan setup Spark di kontainer Flask
            # print("INFO: SparkSession initialized for Flask API.")
            # Untuk sekarang, kita tidak akan inisialisasi Spark di sini, tapi di endpoint jika perlu.
            # Atau lebih baik, GUNAKAN PANDAS + PYARROW jika memungkinkan.
            pass
        except Exception as e:
            print(f"ERROR: Failed to initialize SparkSession in Flask: {e}")
            # spark_session_instance = None # Pastikan tetap None jika gagal
    # return spark_session_instance
    return None # Return None karena kita akan pakai Pandas di endpoint

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

# --- Endpoint untuk Data Gold ---
@app.route('/api/gold_data/country_summary', methods=['GET'])
def get_country_summary_data():
    import pandas as pd

    storage_options = {
        'key': MINIO_ACCESS_KEY,
        'secret': MINIO_SECRET_KEY,
        'client_kwargs': {'endpoint_url': MINIO_ENDPOINT},
        'config_kwargs': {'s3': {'addressing_style': 'path'}} # Penting untuk MinIO
    }

    try:
        print(f"INFO: Attempting to read Delta table from: {GOLD_COUNTRY_YEARLY_SUMMARY_PATH}")
        df = pd.read_parquet(GOLD_COUNTRY_YEARLY_SUMMARY_PATH, storage_options=storage_options)
        
        limit = request.args.get('limit', default=None, type=int)
        if limit:
            df = df.head(limit)
            
        result = df.to_dict(orient='records')
        print(f"INFO: Successfully read {len(result)} records from country_summary.")
        return jsonify(result)
    except Exception as e:
        print(f"ERROR: Failed to read country_summary data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/gold_data/ml_features', methods=['GET'])
def get_ml_features_data():
    import pandas as pd
    storage_options = {
        'key': MINIO_ACCESS_KEY,
        'secret': MINIO_SECRET_KEY,
        'client_kwargs': {'endpoint_url': MINIO_ENDPOINT},
        'config_kwargs': {'s3': {'addressing_style': 'path'}}
    }
    try:
        print(f"INFO: Attempting to read Delta table from: {GOLD_ML_FEATURES_PATH}")
        df = pd.read_parquet(GOLD_ML_FEATURES_PATH, storage_options=storage_options)
        limit = request.args.get('limit', default=None, type=int)
        if limit:
            df = df.head(limit)
        result = df.to_dict(orient='records')
        print(f"INFO: Successfully read {len(result)} records from ml_features.")
        return jsonify(result)
    except Exception as e:
        print(f"ERROR: Failed to read ml_features data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# --- Placeholder untuk Endpoint Prediksi Model ---
@app.route('/predict', methods=['POST'])
def predict():
    # data = request.get_json()
    # TODO: Logika untuk memuat model dari MinIO dan melakukan prediksi
    # model = load_model_from_minio(...)
    # prediction = model.predict(data_features)
    # return jsonify({'prediction': prediction.tolist()})
    return jsonify({
        "message": "Predictive endpoint is under construction.",
        "received_data": request.get_json() if request.is_json else "No JSON data received."
    }), 501 # 501 Not Implemented

if __name__ == '__main__':
    # Port 5000 adalah default Flask, akan dipetakan di Docker Compose
    app.run(host='0.0.0.0', port=5000, debug=True)