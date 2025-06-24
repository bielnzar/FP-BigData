FROM bitnami/spark:3.4

USER root

RUN apt-get update && apt-get install -y curl python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark_apps

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /opt/bitnami/spark/jars-extra
WORKDIR /opt/bitnami/spark/jars-extra

RUN curl -L -o hadoop-aws-3.3.4.jar "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
RUN curl -L -o aws-java-sdk-bundle-1.12.262.jar "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

ENV SPARK_CLASSPATH="/opt/bitnami/spark/jars-extra/*:/opt/bitnami/spark/jars/*"

WORKDIR /opt/spark_apps

USER 1001
