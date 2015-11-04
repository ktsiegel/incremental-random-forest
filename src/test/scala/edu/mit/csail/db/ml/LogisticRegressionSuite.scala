package edu.mit.csail.db.ml

import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite


class TestDb extends ModelDb {
  var fromCache: Boolean = false
  override def getOrElse[M <: Model[M]](spec: ModelSpec[M])(orElse: ()=> M): M = {
    if (contains(spec)) fromCache = true
    super.getOrElse(spec)(orElse)
  }
}
/**
 * Example test case
 */
class LogisticRegressionSuite extends FunSuite {
  /**
   * Very basic test that checks that we can correctly use dataframes and paramMaps from spark.ml
   * Taken from spark.ml example at http://spark.apache.org/docs/latest/ml-guide.html#example-estimator-transformer-and-param
   */
  test("spark.ml dataframe test") {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("simpletest")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    val training = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    // Train a Wahoo logistic regression model.
    val db = new TestDb()
    val lr = new WahooLogisticRegression()
    lr.setMaxIter(10).setRegParam(1.0).setDb(db)
    lr.fit(training)

    // The first training should train from scratch.
    assert(!db.fromCache)

    // The second training should just read from the cache.
    lr.fit(training)
    assert(db.fromCache)
  }
}
