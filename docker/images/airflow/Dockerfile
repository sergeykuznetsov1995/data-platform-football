FROM apache/airflow:2.7.3-python3.11

USER root

# Install Java and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    netcat-openbsd \
    git \
    && rm -rf /var/lib/apt/lists/*

# Chrome/Chromium for Selenium (headless browser automation)
# Xvfb for virtual display (bypasses Cloudflare headless detection)
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libxkbcommon0 \
    xdg-utils \
    xvfb \
    xauth \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    chmod -R 755 ${SPARK_HOME}

ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Download PostgreSQL JDBC driver for Spark
RUN curl -fsSL "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar" \
    -o ${SPARK_HOME}/jars/postgresql-42.7.4.jar

# Iceberg Spark runtime for PySpark jobs
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar" \
    -o ${SPARK_HOME}/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# AWS bundle for S3 compatibility
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar" \
    -o ${SPARK_HOME}/jars/iceberg-aws-bundle-1.4.3.jar

# Create directories
RUN mkdir -p /opt/airflow/spark_jobs /opt/airflow/dags /opt/airflow/logs /opt/airflow/scrapers

# Add custom paths to PYTHONPATH for DAG imports
ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/scrapers:/opt/airflow:${PYTHONPATH}"

# Switch to airflow user for pip installs
USER airflow

# ===== СТАДИЯ 1: Airflow 2.7.x (уже включен в базовый образ) =====
# Базовый образ apache/airflow:2.7.3-python3.11 уже содержит Airflow
# Дополнительная установка Airflow не требуется

# ===== СТАДИЯ 2: Core dependencies (Airflow providers, data processing) =====
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ===== СТАДИЯ 3: Scraping dependencies (installed separately to avoid conflicts) =====
COPY requirements-scraping.txt /tmp/requirements-scraping.txt
RUN pip install --no-cache-dir -r /tmp/requirements-scraping.txt

# Set working directory
WORKDIR /opt/airflow
