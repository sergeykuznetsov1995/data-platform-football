#!/usr/bin/env python3
"""
Create Iceberg tables from Parquet files using PySpark.
"""

from pyspark.sql import SparkSession
import os

# HDFS Parquet file locations
HDFS_BRONZE_DIR = "hdfs://namenode:9000/data/bronze/fbref"

# Table mappings
TABLES = {
    # Player stats
    "player_stats": "fbref_player_standard",
    "player_shooting": "fbref_player_shooting",
    "player_passing": "fbref_player_passing",
    "player_passing_types": "fbref_player_passing_types",
    "player_gca": "fbref_player_gca",
    "player_defense": "fbref_player_defense",
    "player_possession": "fbref_player_possession",
    "player_playingtime": "fbref_player_playingtime",
    "player_misc": "fbref_player_misc",
    # Team stats
    "team_stats": "fbref_team_standard",
    "team_shooting": "fbref_team_shooting",
    "team_passing": "fbref_team_passing",
    "team_passing_types": "fbref_team_passing_types",
    "team_gca": "fbref_team_gca",
    "team_defense": "fbref_team_defense",
    "team_possession": "fbref_team_possession",
    "team_playingtime": "fbref_team_playingtime",
    "team_misc": "fbref_team_misc",
    # Keeper stats
    "keeper_keeper": "fbref_keeper_stats",
    "keeper_keeper_adv": "fbref_keeper_adv",
    # Schedule
    "schedule": "fbref_schedule",
}


def create_spark_session():
    """Create Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("CreateIcebergTables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "hdfs://namenode:9000/user/hive/warehouse") \
        .getOrCreate()


def sanitize_column_name(name: str) -> str:
    """Sanitize column name for SQL."""
    name = name.lower()
    name = name.replace(' ', '_')
    name = name.replace('-', '_')
    name = name.replace('+', '_plus_')
    name = name.replace('/', '_per_')
    name = name.replace('%', '_pct')
    name = name.replace('.', '_')
    name = name.replace('(', '_')
    name = name.replace(')', '')
    if name[0].isdigit():
        name = 'col_' + name
    return name


def load_parquet_to_iceberg(spark, parquet_name: str, table_name: str):
    """Load a Parquet file into Iceberg table."""
    parquet_path = f"{HDFS_BRONZE_DIR}/{parquet_name}.parquet"
    full_table_name = f"iceberg.bronze.{table_name}"

    print(f"\n{'='*60}")
    print(f"Loading: {parquet_path}")
    print(f"Target:  {full_table_name}")
    print('='*60)

    try:
        # Read parquet
        df = spark.read.parquet(parquet_path)
        row_count = df.count()
        print(f"Read {row_count} rows, {len(df.columns)} columns")

        if row_count == 0:
            print("Empty dataframe, skipping")
            return True

        # Sanitize column names
        for col in df.columns:
            safe_col = sanitize_column_name(col)
            if safe_col != col:
                df = df.withColumnRenamed(col, safe_col)

        # Drop existing table
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception as e:
            print(f"Note: {e}")

        # Write to Iceberg
        df.writeTo(full_table_name).createOrReplace()

        # Verify
        count = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
        print(f"✓ Created {full_table_name} with {count} rows")
        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def main():
    """Main entry point."""
    print("="*60)
    print("FBref Parquet -> Iceberg Table Creator")
    print("="*60)

    spark = create_spark_session()

    # Ensure bronze schema exists
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.bronze")

    success = 0
    failed = 0

    for parquet_name, table_name in TABLES.items():
        if load_parquet_to_iceberg(spark, parquet_name, table_name):
            success += 1
        else:
            failed += 1

    print(f"\n{'='*60}")
    print(f"SUMMARY: {success} success, {failed} failed")
    print("="*60)

    spark.stop()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit(main())
