# 1 executors
spark-submit --conf spark.default.parallelism=1 \
--conf spark.sql.shuffle.partitions=1 \
--jars target/scala-2.11/graphframes-0.7.0-spark2.4-s_2.11.jar \
--class com.wrapper.BoruvkaAlgorithm \
--master spark://laptop2017.local:7077 \
--total-executor-cores 1 \
--executor-cores 1 \
--executor-memory 1GB \
"target/scala-2.11/boruvka_2.11-0.1-SNAPSHOT.jar" graphsParquet/graph16

# 2 executors
spark-submit --conf spark.default.parallelism=2 \
--conf spark.sql.shuffle.partitions=2 \
--jars target/scala-2.11/graphframes-0.7.0-spark2.4-s_2.11.jar \
--class com.wrapper.BoruvkaAlgorithm \
--master spark://laptop2017.local:7077 \
--total-executor-cores 2 \
--executor-cores 1 \
--executor-memory 1GB \
"target/scala-2.11/boruvka_2.11-0.1-SNAPSHOT.jar" graphsParquet/graph16

# 3 executors
spark-submit --conf spark.default.parallelism=3 \
--conf spark.sql.shuffle.partitions=3 \
--jars target/scala-2.11/graphframes-0.7.0-spark2.4-s_2.11.jar \
--class com.wrapper.BoruvkaAlgorithm \
--master spark://laptop2017.local:7077 \
--total-executor-cores 3 \
--executor-cores 1 \
--executor-memory 1GB \
"target/scala-2.11/boruvka_2.11-0.1-SNAPSHOT.jar" graphsParquet/graph16