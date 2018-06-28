# Set your environment
NUMCORE_MAC=`sysctl -e hw.ncpu | awk '{print $2}' 2>/dev/null`
NUMCORE_LINUX=`grep -c processor /proc/cpuinfo 2>/dev/null`
NUMCORE=`echo $NUMCORE_MAC $NUMCORE_LINUX | awk '{print $1}'`
MASTER=local[$NUMCORE]
SPARK_HOME=spark-2.3.1
HDFS_HOME=./

$SPARK_HOME/bin/spark-submit --master $MASTER --class kr.acon.TrillionG --jars lib/fastutil-7.0.12.jar,lib/dsiutils-2.3.3.jar TrillionG.jar -hdfs $HDFS_HOME -m $NUMCORE $@