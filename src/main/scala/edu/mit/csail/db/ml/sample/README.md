To run the sample app, build the project:

```
sbt clean && sbt package
```

Then run `spark-submit` (in the command below, we assume that `spark-submit` is in your PATH)

```
spark-submit --master local[4] --class "edu.mit.csail.db.ml.sample.SampleApp" target/scala-2.10/ml-project_2.10-1.0.jar
```