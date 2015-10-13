package edu.mit.csail.db.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class EstimatorSuite extends FunSuite{

  /**
   * Very basic test that checks that we can correctly use dataframes and paramMaps from spark.ml
   * Taken from spark.ml example at http://spark.apache.org/docs/latest/ml-guide.html#example-estimator-transformer-and-param
   */
  test("spark.ml dataset test") {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext._
    val training = sqlContext.read.json("src/main/resources/test.json")

    //training.columns
  }
}
