## MEng project

This repo contains a Spark implementation of incremental random forests.

Run:
```
# sbt assembly
# PATH_TO_SPARK/bin/spark-submit --master local[2] --class "org.apache.spark.ml.wahoo.BENCHMARK" --driver-memory 2g target/scala-2.10/ml.jar > output.txt
