package org.apache.spark.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.{FunSuite, BeforeAndAfter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.classification.{WahooLogisticRegression, LogisticRegressionModel}

/**
 * Check whether models are cached in the model database.
 */
class IncrementalTrainingSuite extends FunSuite with BeforeAndAfter {
  /**
   * Dataset source: UCI Machine Learning Repository
   */
  test("incremental training works") {
    val data = TestBase.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/test/scala/edu/mit/csail/db/ml/data/test.csv")

    TestBase.withContext { (wctx) =>
      val toVec = udf[Vector, Int, Double, Double, Double, Double, Int] {
        (a,b,c,d,e,f) => Vectors.dense(a,b,c,d,e,f)
      }
      val toBinary = udf[Double, Double]( a => {
        if (a > 30.2) 1 // 2011 CAFE standard
        else 0
      })
      val allData = data.withColumn("features", toVec(
        data("cylinders"),
        data("displacement"),
        data("horsepower"),
        data("weight"),
        data("acceleration"),
        data("year")
      )).withColumn("label", toBinary(data("mpg")))
        .select("features","label")
        .randomSplit(Array(0.7,0.1,0.2))

      val training = allData(0)
      val incremental = allData(1)
      val testing = allData(2)

      // Train a Wahoo logistic regression model.
      val lr = wctx.createLogisticRegression
      lr.setMaxIter(2).setRegParam(0.05)
      val model = lr.fit(training)
      val accuracy = evalModel(model, testing)
      assert(accuracy > 0.70) // quality check

      // Train for more iterations and compare to previous model
      lr.setMaxIter(6)
      val deltaIterations = 4
      val modelMoreIter = lr.train(model, training, deltaIterations)
      val accuracyMoreIter = evalModel(modelMoreIter, testing)
      // Should be more accurate than previous model trained with fewer iterations
      assert(accuracyMoreIter >= accuracy)

      // Compare to model trained with same total number of iterations
      val modelTotal = lr.fit(training)
      val accuracyTotal = evalModel(modelTotal, testing)
      // Should be similarly accurate to other model, because same # total iterations
      assert(Math.abs(accuracyTotal - accuracyMoreIter) < 0.1)

      // Compare to model trained with less data
      lr.setMaxIter(2)
      val trainingAndIncremental: DataFrame = training.unionAll(incremental)
      val modelMoreData = lr.train(model, trainingAndIncremental, deltaIterations)
      val accuracyMoreData = evalModel(modelMoreData, testing)
      // Should be more accurate than previous model trained with less data
      assert(accuracyMoreData >= accuracy)
    }
  }

  /**
   * This method evaluates a model on a test data set and outputs both
   * the results and the overall accuracy. This method exists within
   * the LogisticRegressionModel class, but is private, so this is
   * a modified version for testing purposes.
   * @param model - the trained model
   * @param testing - the testing data
   * @return the accuracy of the model on the testing data
   */
  def evalModel(model: LogisticRegressionModel, testing: DataFrame): Double = {
    var count = 0.0
    var pos = 0.0
    model.transform(testing)
      .select("features", "label", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Double, prediction: Double) =>
        count += 1.0
        if (label == prediction) pos += 1.0
      }
    pos/count
  }
}
