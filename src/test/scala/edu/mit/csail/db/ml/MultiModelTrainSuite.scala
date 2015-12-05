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

      // Train the models for the optimized estimator.
      val modelsMulti: Seq[LogisticRegressionModel] = lrMulti.fitMulti(training,
        Array(
          ParamMap(lrMulti.maxIter -> 30),
          ParamMap(lrMulti.maxIter -> 30),
          ParamMap(lrMulti.maxIter -> 30)
        )
      )

      // Train the models for the unoptimized estimator.
      val modelsNormal: Seq[LogisticRegressionModel] = lrNormal.fit(training,
        Array(
          ParamMap(lrNormal.maxIter -> 30),
          ParamMap(lrNormal.maxIter -> 30),
          ParamMap(lrNormal.maxIter -> 30)
        )
      )

      // Ensure that the number of models is the same for both.
      assert(modelsNormal.length == modelsMulti.length)

      modelsNormal.foreach(m => {
        println("Model: " + m.intercept + m.weights)
      })

      modelsMulti.foreach(m => {
        println("Model: " + m.intercept + m.weights)
      })

      // Ensure that all of the models are correct.
      val expectedModel = modelsNormal.head

      modelsNormal.foreach({ (m) =>
        assert(m.intercept == expectedModel.intercept)
        assert(m.weights == expectedModel.weights)
      })

      modelsMulti.foreach({ (m) =>
        assert(m.intercept == expectedModel.intercept)
        assert(m.weights == expectedModel.weights)
      })

    }
  }
}