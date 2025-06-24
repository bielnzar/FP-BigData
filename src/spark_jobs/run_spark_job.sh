#!/bin/bash

if [ -z "$1" ]; then
  echo "Error: Nama skrip pekerjaan Spark tidak diberikan."
  echo "Penggunaan: ./run_spark_job.sh <nama_skrip.py>"
  echo "Contoh:   ./run_spark_job.sh bronze_to_silver.py"
  exit 1
fi

SPARK_JOB_SCRIPT=$1
SPARK_JOB_PATH="/opt/spark_apps/src/spark_jobs/${SPARK_JOB_SCRIPT}"

echo "======================================================"
echo "Mengirimkan pekerjaan Spark: ${SPARK_JOB_SCRIPT}"
echo "Path di dalam kontainer: ${SPARK_JOB_PATH}"
echo "======================================================"

PACKAGES="io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

docker compose exec spark-master spark-submit \
  --packages ${PACKAGES} \
  "${SPARK_JOB_PATH}"

if [ $? -eq 0 ]; then
  echo "======================================================"
  echo "Pekerjaan Spark berhasil dikirim."
  echo "======================================================"
else
  echo "======================================================"
  echo "Terjadi kesalahan saat mengirimkan pekerjaan Spark."
  echo "======================================================"
fi