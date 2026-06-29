FROM apache/hive:4.0.0

USER root

# Download PostgreSQL JDBC driver using ADD
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN chmod 644 /opt/hive/lib/postgresql-42.7.4.jar

# Iceberg Hive runtime
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.4.3/iceberg-hive-runtime-1.4.3.jar /opt/hive/lib/iceberg-hive-runtime-1.4.3.jar
RUN chmod 644 /opt/hive/lib/iceberg-hive-runtime-1.4.3.jar

USER hive
