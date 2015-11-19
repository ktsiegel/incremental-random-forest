package org.apache.spark.ml

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.log4j.{Logger, Level}
import java.io.File

object TestBase {
  // Turn off logging.
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("breeze").setLevel(Level.OFF)

  // Make the log directory, if it doesn't already exist.
  var logDir = "testLog"
  new File(logDir).mkdir()

  // Set up Spark.
  private val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
    .set("spark.eventLog.enabled", "true")
    .set("spark.eventLog.dir", logDir)
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)

  // Connect to ModelDB.
  val db = new TestDb(TestConfig.databaseName, TestConfig.modelsCollection)
}
