
# Set your environment
MASTER=`hostname -f` 
SPARK_HOME=${SPARK_HOME} 
HDFS_HOME=`hadoop fs -df | grep hdfs | awk '{print $1}'`/user/$USER/

bash $SPARK_HOME/bin/spark-submit --master spark://$MASTER:7077 --class kr.acon.TrillionG --jars lib/fastutil-7.0.12.jar,lib/dsiutils-2.3.3.jar TrillionG.jar -hdfs $HDFS_HOME $@ 
