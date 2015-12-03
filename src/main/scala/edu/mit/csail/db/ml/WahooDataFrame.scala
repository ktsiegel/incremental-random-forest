package org.apache.spark.ml

import org.apache.spark.sql.DataFrame
import scala.util.Random

class WahooDataFrame(val dataset: DataFrame) {
  val rand: Random = new Random()
  val uid: java.lang.Integer = rand.nextInt() 
}
