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
class WahooDataFrameSuite extends FunSuite with BeforeAndAfter {

  test("models cached in model database") {
    val training = TestBase.sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    

    // Train a Wahoo logistic regression model.
    val lr = TestBase.wcontext.createLogisticRegression
    lr.setMaxIter(10).setRegParam(1.0)

    val trainingData = new WahooDataFrame(training)
    val model = lr.fit(trainingData)
  }
}
