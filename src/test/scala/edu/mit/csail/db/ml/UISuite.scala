package org.apache.spark.ml

import java.awt.Desktop
import java.net.URI

import org.scalatest.{FunSuite, BeforeAndAfter}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.classification.{LogisticRegressionModel}


/** Check whether UI displays at all. */
class UISuite extends FunSuite with BeforeAndAfter {
  /*
  before {
    TestBase.wcontext.resetDb
  }

  // display Wahoo UI
  test("launch spark ui") {
    // Desktop.getDesktop().browse(new URI("http://localhost:" +
    //   TestBase.wcontext.wahooUI.port))
    TestBase.wcontext.wahooUI.log("hello");
  }
  */

  /**
   * Dataset source: UCI Machine Learning Repository
   */
  /*
  test("model caching works with a large dataset") {
    val data = TestBase.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("src/test/scala/edu/mit/csail/db/ml/data/test.csv")

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
      .randomSplit(Array(0.8,0.2))

    val training = allData(0)
    val testing = allData(1)

    // Train a Wahoo logistic regression model.
    val lr = TestBase.wcontext.createLogisticRegression
    lr.setMaxIter(30).setRegParam(0.05)
    val (model1, fromCache1) = lr.fitTest(training)

    // The first training should train from scratch.
    assert(!fromCache1)
    val accuracy1 = evalModel(model1, testing)

    // The second training should just read from the cache.
    val (model2, fromCache2) = lr.fitTest(training)
    assert(fromCache2)
    val accuracy2 = evalModel(model2, testing)

    assert(accuracy2 == accuracy1)
  }
  */


  /**
   * This method evaluates a model on a test data set and outputs both
   * the results and the overall accuracy. This method exists within
   * the LogisticRegressionModel class, but is private, so this is
   * a modified version for testing purposes.
   * @param model - the trained model
   * @param testing - the testing data
   * @return the accuracy of the model on the testing data
   */
  /*
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
  */


}
