SHELL := /bin/bash

.PHONY: up down ps logs init-hdfs smoke-trino smoke-spark smoke-hive

up:
	docker compose up -d postgres namenode datanode hive-metastore trino spark airflow

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=200

init-hdfs:
	docker compose exec -T namenode bash -lc "/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /data/{raw,silver,gold} && /opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /data | sed -n '1,200p'"

smoke-hive:
	docker compose exec -T namenode bash -lc "/opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /data/raw/test_table"
	docker compose exec -T hive-metastore bash -lc "/opt/hive/bin/hive -e 'CREATE DATABASE IF NOT EXISTS demo; CREATE EXTERNAL TABLE IF NOT EXISTS demo.test_table (id INT) STORED AS PARQUET LOCATION \"hdfs://namenode:9000/data/raw/test_table\";' --hiveconf hive.metastore.uris=thrift://hive-metastore:9083"

smoke-trino:
	docker compose exec -T trino bash -lc "/usr/lib/trino/bin/trino --execute 'show catalogs' | sed -n '1,200p'"

smoke-spark:
	docker compose exec -T spark bash -lc "cat > /tmp/smoke.py <<'PY'\nfrom pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\ndf = spark.createDataFrame([(1, 'a')], ['id','v'])\ndf.write.mode('overwrite').parquet('hdfs://namenode:9000/data/silver/smoke')\nprint(df.count())\nPY\n/opt/bitnami/spark/bin/spark-submit /tmp/smoke.py"
