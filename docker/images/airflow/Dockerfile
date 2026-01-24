FROM apache/airflow:2.10.4-python3.11

USER root

# Install Java and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

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

# Create directories
RUN mkdir -p /opt/airflow/spark_jobs /opt/airflow/dags /opt/airflow/logs

# Switch to airflow user for pip installs
USER airflow

# Copy and install Python requirements
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# Set working directory
WORKDIR /opt/airflow
