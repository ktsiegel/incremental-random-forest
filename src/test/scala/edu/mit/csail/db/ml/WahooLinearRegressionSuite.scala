package org.apache.spark.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.{FunSuite, BeforeAndAfter}


class WahooLinearRegressionSuite extends FunSuite with BeforeAndAfter {
  before {
    TestBase.wcontext.resetDb
  }

  test("are models cached in the model database?") {
    val training = TestBase.sqlContext.createDataFrame(Seq(
      (34.0, Vectors.dense(0.0, 1.1, 0.1)),
      (6.0, Vectors.dense(2.0, 1.0, -1.0)),
      (5.0, Vectors.dense(2.0, 1.3, 1.0)),
      (11.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")


    // Train a Wahoo Linear regression model.
    val lr = TestBase.wcontext.createLinearRegression
    lr.setMaxIter(10).setRegParam(1.0)
    val (_, fromCache) = lr.fitTest(training)

    // The first training should train from scratch.
    assert(!fromCache)

   // The second training should just read from the cache.
    val (_, fromCache2) = lr.fitTest(training) // hack something weird about reusing tuples
    assert(fromCache2)
  }
}
