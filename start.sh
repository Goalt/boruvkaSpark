# 1 executors
/spark-2.4.7-bin-hadoop2.7/bin/spark-submit --conf spark.default.parallelism=1 \
--conf spark.sql.shuffle.partitions=1 \
--jars target/scala-2.11/graphframes-0.7.0-spark2.4-s_2.11.jar \
--class com.wrapper.BoruvkaAlgorithm \
--master local[8] \
--total-executor-cores 1 \
--executor-cores 1 \
--executor-memory 1GB \
"target/scala-2.11/app_2.11-0.1-SNAPSHOT.jar" graphsParquet/graph16