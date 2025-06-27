import os
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

MINIO_ENDPOINT_URL_DOCKER = os.environ.get("MINIO_ENDPOINT_URL_DOCKER", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MODEL_PATH = "s3a://models/mortality-rate-predictor"

def get_spark_session():
    """
    Menginisialisasi sesi Spark untuk berjalan dalam mode lokal, dikonfigurasi untuk MinIO.
    Ini lebih andal untuk API karena tidak bergantung pada Spark master eksternal.
    """
    return (
        SparkSession.builder.appName("PredictionAPI")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_URL_DOCKER)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

def load_model(spark, path):
    """Memuat model Spark ML Pipeline yang sudah dilatih."""
    try:
        model = PipelineModel.load(path)
        print(f"Model berhasil dimuat dari {path}")
        return model
    except Exception as e:
        print(f"Gagal memuat model dari {path}: {e}")
        return None

app = Flask(__name__)
spark_session = get_spark_session()
model = load_model(spark_session, MODEL_PATH)

prediction_schema = StructType([
    StructField("Year", IntegerType(), True),
    StructField("Prevalence_Rate_Percent", FloatType(), True),
    StructField("Incidence_Rate_Percent", FloatType(), True),
    StructField("Average_Treatment_Cost_USD", IntegerType(), True),
    StructField("Recovery_Rate_Percent", FloatType(), True),
    StructField("DALYs", FloatType(), True),
    StructField("Per_Capita_Income_USD", IntegerType(), True),
    StructField("Country", StringType(), True),
    StructField("Disease_Category", StringType(), True),
    StructField("Age_Group", StringType(), True),
    StructField("Gender", StringType(), True)
])

@app.route('/health', methods=['GET'])
def health():
    """Endpoint untuk health check."""
    if model:
        return jsonify({'status': 'OK', 'model': 'loaded'})
    else:
        return jsonify({'status': 'ERROR', 'model': 'not loaded'}), 500

@app.route('/predict', methods=['POST'])
def predict():
    """Endpoint untuk prediksi."""
    if not model:
        return jsonify({'error': 'Model tidak dimuat'}), 500

    try:
        data = request.get_json()

        try:
            assembler = next(s for s in reversed(model.stages) if isinstance(s, VectorAssembler))
            model_numeric_cols = {c for c in assembler.getInputCols() if not c.endswith('_vec')}

            model_categorical_cols = {s.getInputCol() for s in model.stages if isinstance(s, StringIndexer)}
            
            expected_features = model_numeric_cols.union(model_categorical_cols)
        except (StopIteration, AttributeError):
            return jsonify({'error': 'Tidak dapat mengidentifikasi fitur yang dibutuhkan dari pipeline model.'}), 500

        request_cols = set(data.keys())

        missing_features = expected_features - request_cols
        if missing_features:
            error_msg = (
                f"Fitur yang dibutuhkan oleh model tidak ada dalam request: {sorted(list(missing_features))}. "
                "Ini kemungkinan terjadi karena model yang dimuat tidak sinkron dengan kode aplikasi. "
                "Solusi: Latih ulang model (jalankan 'bash src/spark_jobs/run_spark_job.sh train_model.py') lalu restart layanan API."
            )
            return jsonify({'error': error_msg}), 400 # Bad Request

        df = spark_session.createDataFrame([data], schema=prediction_schema)
        
        prediction_df = model.transform(df)
        prediction = prediction_df.select("prediction").first()[0]
        
        return jsonify({'mortality_rate_prediction': prediction})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
