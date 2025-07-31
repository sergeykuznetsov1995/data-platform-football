FROM apache/hive:3.1.3

USER root

# Установка PostgreSQL клиента и JDBC драйвера
RUN apt-get update && apt-get install -y postgresql-client wget

# Скачиваем PostgreSQL JDBC драйвер
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -O /opt/hive/lib/postgresql-42.2.18.jar

# Копирование скрипта запуска
COPY start-metastore.sh /opt/hive/bin/
RUN chmod +x /opt/hive/bin/start-metastore.sh

# Создание директории для логов
RUN mkdir -p /opt/hive/logs

EXPOSE 9083

CMD ["/opt/hive/bin/start-metastore.sh"] 