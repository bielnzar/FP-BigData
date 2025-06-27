#!/bin/bash

set -e

if [ -z "$1" ]; then
  echo "Error: Nama file skrip Spark harus diberikan."
  echo "Contoh: bash $0 bronze_to_silver.py"
  exit 1
fi

SPARK_JOB_SCRIPT=$1
SPARK_MASTER_CONTAINER="spark-master"

echo "======================================================"
echo "Mengirimkan pekerjaan Spark: $SPARK_JOB_SCRIPT"
echo "======================================================"

# Flag --packages dihilangkan karena JARs sudah ada di dalam base image Docker
docker exec "$SPARK_MASTER_CONTAINER" spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  "/opt/spark_apps/src/spark_jobs/$SPARK_JOB_SCRIPT"

if [ $? -eq 0 ]; then
  echo "======================================================"
  echo "Pekerjaan Spark '$SPARK_JOB_SCRIPT' berhasil diselesaikan."
  echo "======================================================"
else
  echo "======================================================"
  echo "Terjadi kesalahan saat mengirimkan pekerjaan Spark."
  echo "======================================================"
  exit 1
fi