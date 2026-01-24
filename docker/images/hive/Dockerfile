FROM apache/hive:4.0.0

USER root

# Download PostgreSQL JDBC driver using ADD
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN chmod 644 /opt/hive/lib/postgresql-42.7.4.jar

USER hive
