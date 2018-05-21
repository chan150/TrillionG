
# Set your environment
NUMCORE=`cat /proc/cpuinfo|grep processor| wc -l`
MASTER=local[$NUMCORE]
SPARK_HOME=spark-2.1.0
HDFS_HOME=./

bash $SPARK_HOME/bin/spark-submit --master $MASTER --class kr.acon.TrillionG --jars lib/fastutil-7.0.12.jar,lib/dsiutils-2.3.3.jar TrillionG.jar -hdfs $HDFS_HOME -m $NUMCORE $@ 
