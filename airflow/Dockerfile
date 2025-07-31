FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    wget \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Скачиваем и распаковываем Hadoop
ENV HADOOP_VERSION=3.3.6
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt && \
    rm hadoop-${HADOOP_VERSION}.tar.gz
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV ARROW_LIBHDFS_DIR=$HADOOP_HOME/lib/native

# Добавляем airflow в группу docker
RUN groupadd -g 113 docker || groupmod -g 113 docker && usermod -aG docker airflow

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
