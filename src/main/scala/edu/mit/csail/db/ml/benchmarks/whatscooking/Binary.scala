package edu.mit.csail.db.ml.benchmarks.whatscooking

import edu.mit.csail.db.ml.benchmarks.Timing
import org.apache.spark.ml.classification.{LogisticRegressionModel, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

/**
  * Train a logistic regression model for binary classification on the What's Cooking dataset.
  */
object Binary {
  def main(args: Array[String]): Unit = {
    def simplify = (df: DataFrame) => df.select("feature", "labelIndex").where(df("labelIndex") < 2)
    val (training, test, wc, shouldUseWahoo) = Common.readInput(args)


    val singleClassifier = if (shouldUseWahoo) {
      wc.createLogisticRegression
    } else {
      new LogisticRegression("whatscookingtest_binary")
    }.setMaxIter(100)

    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("labelIndex").setPredictionCol("predict").setMetricName("f1")

    singleClassifier
      .setFeaturesCol("feature")
      .setLabelCol("labelIndex")
      .setPredictionCol("predict")
    val pm1 = ParamMap(
      singleClassifier.elasticNetParam -> 1.0,
      singleClassifier.regParam -> 0.1,
      singleClassifier.fitIntercept -> true
    )
    val pm2 = ParamMap(
      singleClassifier.elasticNetParam -> 0.1,
      singleClassifier.regParam -> 0.01,
      singleClassifier.fitIntercept -> true
    )
    val pm3 = ParamMap(
      singleClassifier.elasticNetParam -> 0.01,
      singleClassifier.regParam -> 0.1,
      singleClassifier.fitIntercept -> false
    )

    var models: Seq[LogisticRegressionModel] = Seq()
    Timing.time("Training on three ParamMaps") { () =>
      models = singleClassifier.fit(simplify(training), Array(pm1, pm2, pm3))
    }()

    Timing.time("Evaluating") { () =>
      val f1Scores = models.map((model) => eval.evaluate(model.transform(simplify(test))))
      println("F1 scores " + f1Scores)
    }()
  }
}
