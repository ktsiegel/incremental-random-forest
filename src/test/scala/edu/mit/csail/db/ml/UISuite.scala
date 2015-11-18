package org.apache.spark.ml

import java.awt.Desktop
import java.net.URI

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.{FunSuite, BeforeAndAfter}

/**
 * Check whether models are cached in the model database.
 */
class UISuite extends FunSuite with BeforeAndAfter {
  before {
    TestBase.db.clear()
  }

  // -ui test-
  test("launch spark ui") {	
    // display Spark UI
    Desktop.getDesktop().browse(new URI("http://localhost:4040"))
	
	// display Wahoo UI
    val webServer = new WahooUI(8080, TestBase.sc)
	webServer.display
	
    // train a Wahoo logistic regression model
    val trainingData = TestBase.sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.2, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.1)),
      (0.0, Vectors.dense(2.1, 1.3, 1.0)),
      (1.0, Vectors.dense(0.1, 1.2, -0.5))
    )).toDF("label", "features")	
	
    val lr = new WahooLogisticRegression()
    lr.setMaxIter(10).setRegParam(1.0).setDb(TestBase.db)
    lr.fit(trainingData)
	
    // training should train from scratch
    assert(!TestBase.db.fromCache)
  }
}