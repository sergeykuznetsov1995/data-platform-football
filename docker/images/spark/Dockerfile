FROM apache/spark:3.5.0

USER root

# Install additional tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN curl -fsSL "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar" \
    -o /opt/spark/jars/postgresql-42.7.4.jar

# Create spark-events directory
RUN mkdir -p /tmp/spark-events && chmod 755 /tmp/spark-events

# Create conf directory if not exists
RUN mkdir -p /opt/spark/conf

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV SPARK_NO_DAEMONIZE=true

# Iceberg Spark runtime
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar" \
    -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# AWS bundle for S3 compatibility (optional, but useful)
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar" \
    -o /opt/spark/jars/iceberg-aws-bundle-1.4.3.jar

# Create non-root spark user and set ownership
RUN groupadd -r spark 2>/dev/null || true && \
    useradd -r -g spark -d /opt/spark -s /bin/bash spark 2>/dev/null || true && \
    mkdir -p /opt/spark/work && \
    chown -R spark:spark /opt/spark /tmp/spark-events

WORKDIR /opt/spark

USER spark
