package edu.mit.csail.db.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

/**
 * Example test case
 */
class TestSuite extends FunSuite{

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
    // maybe do something here?
  }

  test() {

  }
}
