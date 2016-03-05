package edu.mit.csail.db.ml.benchmarks.wahoo

import org.apache.spark.ml.{Transformer, Estimator, PipelineStage}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.types.{StringType, StructField, DoubleType, IntegerType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
  * Created by kathrynsiegel on 2/21/16.
  */
object WahooUtils {
  def readData(trainingDataPath: String, sc: SparkContext): DataFrame = {
    // set up contexts
    val sqlContext = new SQLContext(sc)

    // Read data and convert to dataframe
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line of file as header
      .option("inferSchema", "true") // automatically infer data types
      .load(trainingDataPath)
  }

  def processIntColumn(column: String, dataset: DataFrame): DataFrame = {
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

  def processIntColumns(dataset: DataFrame): DataFrame = {
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

  private def makeIndexers(fields: Seq[StructField]): Array[PipelineStage] = {
    fields.toArray.map( (entry) =>
      new StringIndexer()
        .setInputCol(entry.name)
        .setOutputCol(entry.name + "_index")
    )
  }

  private def makeEncoders(fields: Seq[StructField]): Array[PipelineStage] = {
    fields.toArray.map( (entry) =>
      new OneHotEncoder()
        .setInputCol(entry.name + "_index")
        .setOutputCol(entry.name + "_vec")
    )
  }

  def processStringColumns(dataset: DataFrame, fields: Seq[StructField]): Array[PipelineStage] = {
    makeIndexers(fields) ++ makeEncoders(fields)
  }

  def getNumericFields(dataset: DataFrame, label: String): Seq[StructField] = {
    dataset.schema.filter( entry =>
      entry.dataType == DoubleType && entry.name != label)
  }

  def getStringFields(dataset: DataFrame, names: Option[Array[String]]): Seq[StructField] = {
    names match {
      case Some(n) => {
        dataset.schema.filter(entry => entry.dataType == StringType &&
          n.contains(entry.name))
      }
      case None => {
        dataset.schema.filter(entry => entry.dataType == StringType)
      }
    }
  }

  def createAssembler(fields: Array[String]): VectorAssembler = {
    new VectorAssembler()
      .setInputCols(fields)
      .setOutputCol("features")
  }

  def processDataFrame(dataset: DataFrame, stages: Array[PipelineStage]): DataFrame = {
    var df = dataset
    stages.foreach(stage =>
      stage match {
        case estimator: Estimator[_] =>
          df = estimator.fit(df).transform(df)
        case transformer: Transformer =>
          df = transformer.transform(df)
    })
    df
  }
}
