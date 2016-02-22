package edu.mit.csail.db.ml.benchmarks.wahoo

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Created by kathrynsiegel on 2/21/16.
  */
object WahooUtils {
  def readData(trainingDataPath: String, appName: String, master: String): DataFrame = {
    // set up contexts
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Read data and convert to dataframe
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line of file as header
      .option("inferSchema", "true") // automatically infer data types
      .load(trainingDataPath)
  }

  def columnIntToDouble(column: String, dataset: DataFrame): DataFrame = {
    val toDouble = org.apache.spark.sql.functions.udf[Double, Int](intLabel => intLabel.asInstanceOf[Double])
    dataset.withColumn(column, toDouble(dataset(column)))
  }

  def createStringIndexer(input: String, output: String): StringIndexer = {
    new StringIndexer().setInputCol(input).setOutputCol(output)
  }

  def createEvaluator(labelCol: String, predictionCol: String) = {
    new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol(predictionCol)
      .setMetricName("precision")
  }

  def columnsIntToDouble(dataset: DataFrame): DataFrame = {
    var df = dataset
    val toDouble = org.apache.spark.sql.functions.udf[Double, Int](intLabel => intLabel.asInstanceOf[Double])
    df.schema.foreach( entry => {
      entry.dataType match {
        case IntegerType => {
          df = df.withColumn(entry.name, toDouble(df(entry.name)))
        }
        case _ => {}
      }
    })
    df
  }

}
