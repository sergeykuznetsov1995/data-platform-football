"""
Data Platform - Spark Integration Test
=======================================
Simple test to verify Spark cluster and HDFS connectivity.
"""

from pyspark.sql import SparkSession
import sys


def main():
    print("=" * 60)
    print("Spark Integration Test")
    print("=" * 60)

    # Create Spark session
    spark = SparkSession.builder \
        .appName("HelloWorld-Test") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    print(f"\nSpark Version: {spark.version}")
    print(f"Spark Master: {spark.sparkContext.master}")
    print(f"App Name: {spark.sparkContext.appName}")

    # Test 1: Basic DataFrame operations
    print("\n--- Test 1: Basic DataFrame Operations ---")
    data = [
        ("Lionel Messi", "Inter Miami", 10),
        ("Cristiano Ronaldo", "Al Nassr", 7),
        ("Kylian Mbappe", "Real Madrid", 9),
        ("Erling Haaland", "Man City", 9),
    ]
    columns = ["player", "team", "number"]

    df = spark.createDataFrame(data, columns)
    df.show()
    print(f"Row count: {df.count()}")

    # Test 2: HDFS connectivity
    print("\n--- Test 2: HDFS Connectivity ---")
    try:
        # Write test data to HDFS
        test_path = "hdfs://namenode:9000/tmp/spark_test"
        df.write.mode("overwrite").parquet(test_path)
        print(f"Successfully wrote data to: {test_path}")

        # Read it back
        df_read = spark.read.parquet(test_path)
        print(f"Successfully read data back: {df_read.count()} rows")

        # Cleanup
        spark._jvm.org.apache.hadoop.fs.FileSystem \
            .get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(test_path), True)
        print("Cleanup completed")
    except Exception as e:
        print(f"HDFS test failed: {e}")
        spark.stop()
        sys.exit(1)

    # Test 3: Hive Metastore connectivity (optional)
    print("\n--- Test 3: Hive Metastore Connectivity ---")
    try:
        databases = spark.sql("SHOW DATABASES").collect()
        print(f"Available databases: {[db.databaseName for db in databases]}")
    except Exception as e:
        print(f"Hive Metastore test: {e}")
        print("(This may be expected if Metastore is not fully initialized)")

    # Summary
    print("\n" + "=" * 60)
    print("Spark integration test passed!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
