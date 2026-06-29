#!/bin/bash
set -e

# Format namenode if not formatted
if [ "$1" = "hdfs" ] && [ "$2" = "namenode" ]; then
    if [ ! -d "/hadoop/dfs/name/current" ]; then
        echo "Formatting NameNode..."
        hdfs namenode -format -force -nonInteractive
    fi
fi

exec "$@"
