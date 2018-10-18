#!/usr/bin/env bash

NUMCORE_MAC=`sysctl -e hw.ncpu | awk -F= '{print $2}' 2>/dev/null`
NUMCORE_LINUX=`grep -c processor /proc/cpuinfo 2>/dev/null`
NUMCORE=`echo $NUMCORE_MAC $NUMCORE_LINUX | awk '{print $1}'`

# Set your environment
MASTER=local[$NUMCORE]
SPARK_HOME=spark-2.3.2
HDFS_HOME=./

# Set your environment
MEMORY=512g

$SPARK_HOME/bin/spark-submit --master $MASTER --class kr.acon.ApplicationMain \
 --executor-memory $MEMORY --driver-memory $MEMORY --conf spark.network.timeout=20000000ms \
 --conf spark.hadoop.dfs.replication=1 \
 --jars lib/fastutil-8.1.1.jar,lib/dsiutils-2.4.2.jar \
 TrillionG.jar TrillionG -hdfs $HDFS_HOME -format tsv -machine $NUMCORE $@