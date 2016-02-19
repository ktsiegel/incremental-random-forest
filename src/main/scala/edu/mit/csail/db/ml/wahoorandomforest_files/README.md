To run:

> sbt assembly
> PATH_TO_SPARK/bin/spark-submit --class "edu.mit.csail.db.ml.benchmarks.homesite.WahooRandomForestIncremental" target/scala-2.10/ml.jar PATH_TO_HOMESITE_CSV
