which spark-submit > /dev/null
if test $? -ne 0 ; then
    echo "[ERROR] Spark must be set in your environment (spark-submit must be accessible)"
    exit -1
fi

which hadoop > /dev/null
if test $? -ne 0 ; then
    echo "[ERROR] Hadoop must be set in your environment (in order to use HDFS)"
    exit -1
fi

# Set your environment
MASTER=`hostname -f`
PORT=7077
HDFS_HOME=`hadoop fs -df | grep hdfs | awk '{print $1}'`/user/$USER/

spark-submit --master spark://$MASTER:$PORT --class kr.acon.TrillionG --jars lib/fastutil-7.0.12.jar,lib/dsiutils-2.3.3.jar TrillionG.jar -hdfs $HDFS_HOME $@
