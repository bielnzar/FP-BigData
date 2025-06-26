FROM fp-bigdata-spark-base:latest

WORKDIR /opt/spark_apps

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
