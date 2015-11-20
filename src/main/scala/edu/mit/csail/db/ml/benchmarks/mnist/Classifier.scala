package edu.mit.csail.db.ml.benchmarks.mnist

import edu.mit.csail.db.ml.benchmarks.Timing
import org.apache.spark.ml.{WahooContext, WahooConfig}
import org.apache.spark.ml.classification.{OneVsRest, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidatorModel, CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object Classifier {
  def main(args: Array[String]): Unit = {
    // The first positional argument is the path to the data file.
    val pathToMNISTDataFile = if (args.length < 1) {
      throw new IllegalArgumentException("Missing path to data file positional argument")
    } else {
      args(0)
    }

    // The second positional argument indicates whether Spark or Wahoo classes should be used.
    val shouldUseWahoo = if (args.length < 2) {
      throw new IllegalArgumentException("Missing [wahoo|spark] positional argument")
    } else {
      args(1) match {
        case "wahoo" => true
        case "spark" => false
        case _ => throw new IllegalArgumentException("[wahoo|spark] argument must be either \"spark\" or \"wahoo\"")
      }
    }

    // Set up contexts.
    val conf = new SparkConf().setAppName("MNIST")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val wahooConfig = new WahooConfig()
    val wahooContext = new WahooContext(sc, wahooConfig)

    // Read the MNIST training data.

    // First, read the text file.
    val rawCsv = sc.textFile(pathToMNISTDataFile)

    // We'll need to get rid of the header.
    val header = rawCsv.first

    // Create and RDD[LabeledPoint]
    val rows = rawCsv
      .filter(_ != header) // Remove the header.
      .map { (line) =>
        val parts = line.split(",")
        val label = parts(0).toDouble
        val features = Vectors.dense(parts.tail.map(_.toDouble)).toSparse
        // Create a labeled point containing the pixels (features) and label (digit).
        new LabeledPoint(label, features)
      }

    // Create a dataframe from the RDD.
    val mnist = sqlContext.createDataFrame[LabeledPoint](rows)

    // Create training and test datasets.
    val split = mnist.randomSplit(Array(0.7, 0.3))
    val (training, test) = (split(0), split(1))
    val singleClassifer = if (shouldUseWahoo) {
      new LogisticRegression("whatscookingtest_classifier")
    } else {
      wahooContext.createLogisticRegression
    }.setMaxIter(100)

    // Create a param grid to search over.
    val params = new ParamGridBuilder()
      .addGrid(singleClassifer.elasticNetParam, Array(1.0, 0.1, 0.01))
      .addGrid(singleClassifer.regParam, Array(0.1, 0.01))
      .addGrid(singleClassifer.fitIntercept, Array(true, false))
      .build()

    // Make a one vs. rest classifier.
    val multiClassifier = new OneVsRest()
      .setPredictionCol("predict")
      .setLabelCol("label")
      .setClassifier(singleClassifer)

    // Create evaluator.
    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("predict")
      .setMetricName("f1")

    // Create cross validator.
    val crossValidator = new CrossValidator()
      .setEstimator(multiClassifier)
      .setEstimatorParamMaps(params)
      .setNumFolds(2)
      .setEvaluator(eval)

    // Find the best model.
    var model: Option[CrossValidatorModel] = None
    Timing.time("Cross validate") { () =>
      model = Some(crossValidator.fit(training))
    }()

    // Compute the F1 score.
    Timing.time("Evaluate") { () =>
      val f1Score = eval.evaluate(model.get.transform(test))
      println("F1 score is " + f1Score)
    }()
  }
}
