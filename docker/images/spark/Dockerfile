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
RUN mkdir -p /tmp/spark-events && chmod 777 /tmp/spark-events

# Create conf directory if not exists
RUN mkdir -p /opt/spark/conf

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV SPARK_NO_DAEMONIZE=true

WORKDIR /opt/spark
