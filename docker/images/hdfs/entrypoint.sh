#!/bin/bash
set -e

# Docker's `USER hadoop` sets the uid but does NOT export $USER; Hadoop's
# hadoop_verify_user_perm compares $HDFS_*_USER against $USER and aborts
# ("can only be executed by ...") when $USER is empty. Normalize it.
export USER="$(id -un)"

# Format namenode if not formatted
if [ "$1" = "hdfs" ] && [ "$2" = "namenode" ]; then
    if [ ! -d "/hadoop/dfs/name/current" ]; then
        echo "Formatting NameNode..."
        hdfs namenode -format -force -nonInteractive
    fi
fi

exec "$@"
