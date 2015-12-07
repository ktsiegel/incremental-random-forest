package org.apache.spark.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.classification.{WahooLogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.{FunSuite, BeforeAndAfter}

class MultiModelTrainSuite extends FunSuite with BeforeAndAfter {
  test("many models trained in one pass") {
    TestBase.withContext { (wctx) =>

      val training = TestBase.sqlContext.createDataFrame(Seq(
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5))
      )).toDF("label", "features")

      // Create an estimator with the optimization and one without.
      val lrMulti = wctx.createLogisticRegression
      val lrNormal = wctx.createLogisticRegression
      val lrTest = wctx.createLogisticRegression

      // Train the models for the optimized estimator.
      val modelsMulti: Seq[LogisticRegressionModel] = lrMulti.fitMulti(training,
        Array(
          ParamMap(lrMulti.maxIter -> 10),
          ParamMap(lrMulti.maxIter -> 20),
          ParamMap(lrMulti.maxIter -> 30)
        )
      )

      // Train the models for the unoptimized estimator.
      val modelsNormal: Seq[LogisticRegressionModel] = lrNormal.fit(training,
        Array(
          ParamMap(lrNormal.maxIter -> 10),
          ParamMap(lrNormal.maxIter -> 20),
          ParamMap(lrNormal.maxIter -> 30)
        )
      )

      lrTest.setMaxIter(30)
      val modelTest = lrTest.fitCopy(training)

      // Ensure that the number of models is the same for both.
      assert(modelsNormal.length == modelsMulti.length)

      modelsNormal.foreach(m => {
        println("Model Normal: " + m.intercept + m.weights)
      })

      modelsMulti.foreach(m => {
        println("Model Multi: " + m.intercept + m.weights)
      })

      println("Model Test: " + modelTest.intercept + modelTest.weights)

      // Ensure that all of the models are correct.
      for (i <- 0 until modelsNormal.length) {
        assert(modelsNormal(i).intercept == modelsMulti(i).intercept)
        assert(modelsNormal(i).weights == modelsMulti(i).weights)
      }
    }
  }
}