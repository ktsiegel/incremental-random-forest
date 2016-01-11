package edu.mit.csail.db.ml.benchmarks.whatscooking

import edu.mit.csail.db.ml.benchmarks.Timing
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidatorModel, CrossValidator, ParamGridBuilder}

/**
  * Train a one vs. rest classifier for the What's cooking dataset.
  */
object Multiclass {
  def main(args: Array[String]): Unit = {
    // Read the data.
    val (training, test, wc, shouldUseWahoo) = Common.readInput(args)

    // Create the basis classifier.
    val singleClassifier = if (shouldUseWahoo) {
      wc.createLogisticRegression
    } else {
      new LogisticRegression
    }.setMaxIter(3)

    // Create the evaluator.
    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("labelIndex").setPredictionCol("predict").setMetricName("f1")

    // Create a grid for cross validation.
    val params = new ParamGridBuilder()
      .addGrid(singleClassifier.elasticNetParam, Array(1.0, 0.1, 0.01))
      .addGrid(singleClassifier.regParam, Array(0.1, 0.01))
      .addGrid(singleClassifier.fitIntercept, Array(true, false))
      .build()

    // Create a one vs. rest classifier from the basis classifier.
    val multiClassifier = new OneVsRest()
      .setFeaturesCol("feature")
      .setLabelCol("labelIndex")
      .setPredictionCol("predict")
      .setClassifier(singleClassifier)

    // Create the cross validator.
    val crossValidator = new CrossValidator()
      .setEstimator(multiClassifier).setEstimatorParamMaps(params).setNumFolds(2).setEvaluator(eval)

    // Find the best model through cross validation on the training set.
    var model: Option[CrossValidatorModel] = None
    Timing.time("Cross validating") { () =>
      model = Some(crossValidator.fit(training))
    }()

    // Evaluate the model on the test set.
    Timing.time("Evaluating model") { () =>
      val f1Score = eval.evaluate(model.get.transform(test))
      println("F1 score is " + f1Score)
    }()
  }
}
