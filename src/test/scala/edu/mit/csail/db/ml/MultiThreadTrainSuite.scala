package org.apache.spark.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{LinearRegressionModel, LinearRegression}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.{FunSuite, BeforeAndAfter}

class TestLRMultiThread(uid: String) extends LinearRegression(uid)
with MultiThreadTrain[LinearRegressionModel] {
  def this() = this(Identifiable.randomUID("linreg"))
}
class MultiThreadTrainSuite extends FunSuite with BeforeAndAfter {
  test("models trained in parallel are same as those trained sequentially") {
    TestBase.withContext { (wctx) =>

      // Training data is y = 2x + 1
      val training = TestBase.sqlContext.createDataFrame(Seq(
        (1.0, Vectors.dense(0.0)),
        (3.0, Vectors.dense(1.0)),
        (5.0, Vectors.dense(2.0)),
        (7.0, Vectors.dense(3.0))
      )).toDF("label", "features")

      // Create an estimator with the optimization and one without.
      val lrMulti = new TestLRMultiThread().setRegParam(1.0)
      val lrNormal = new LinearRegression().setRegParam(1.0)

      // Train the models for the optimized estimator.
      val modelsMulti: Seq[LinearRegressionModel] = lrMulti.fit(training,
        Array(
          ParamMap(lrMulti.maxIter -> 30),
          ParamMap(lrMulti.maxIter -> 40),
          ParamMap(lrMulti.maxIter -> 50)
        )
      )

      // Train the models for the unoptimized estimator.
      val modelsNormal: Seq[LinearRegressionModel] = lrNormal.fit(training,
        Array(
          ParamMap(lrNormal.maxIter -> 30),
          ParamMap(lrNormal.maxIter -> 40),
          ParamMap(lrNormal.maxIter -> 50)
        )
      )

      // Ensure that the number of models is the same for both.
      assert(modelsNormal.length == modelsMulti.length)

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
