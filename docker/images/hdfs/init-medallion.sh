#!/bin/bash
# =============================================================================
# Initialize HDFS with Medallion Architecture directories
# =============================================================================

set -e

echo "Waiting for HDFS to be ready..."
until hdfs dfs -ls / > /dev/null 2>&1; do
    echo "HDFS not ready yet, waiting..."
    sleep 5
done

echo "HDFS is ready. Creating Medallion Architecture directories..."

# Create Bronze layer (raw data)
hdfs dfs -mkdir -p /data/bronze/fbref
hdfs dfs -mkdir -p /data/bronze/transfermarkt
hdfs dfs -mkdir -p /data/bronze/understat
hdfs dfs -mkdir -p /data/bronze/sofascore

# Create Silver layer (cleaned data)
hdfs dfs -mkdir -p /data/silver/players
hdfs dfs -mkdir -p /data/silver/teams
hdfs dfs -mkdir -p /data/silver/matches
hdfs dfs -mkdir -p /data/silver/stats

# Create Gold layer (aggregated data)
hdfs dfs -mkdir -p /data/gold/analytics
hdfs dfs -mkdir -p /data/gold/reports
hdfs dfs -mkdir -p /data/gold/ml_features

# Create Hive warehouse directory
hdfs dfs -mkdir -p /user/hive/warehouse

# Create temp directories
hdfs dfs -mkdir -p /tmp
hdfs dfs -mkdir -p /user/spark/warehouse

# Set permissions
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /user
hdfs dfs -chmod -R 777 /tmp

echo "Medallion Architecture directories created successfully!"
echo ""
echo "Directory structure:"
hdfs dfs -ls -R /data | head -30
