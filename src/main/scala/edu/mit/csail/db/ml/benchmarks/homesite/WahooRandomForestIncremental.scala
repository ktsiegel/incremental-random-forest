package edu.mit.csail.db.ml.benchmarks.homesite

import org.apache.spark.ml.wahoo.WahooRandomForestClassifier
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
  * Created by kathrynsiegel on 2/15/16.
  */
object WahooRandomForestIncremental {
  def main(args: Array[String]) {
    val trainingDataPath = if (args.length < 1) {
      throw new IllegalArgumentException("Missing training data file.")
    } else {
      args(0)
    }

    // set up contexts
    val conf = new SparkConf()
      .setAppName("Homesite")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Read data and convert to dataframe
    val dfInitial = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // use first line of file as header
      .option("inferSchema", "true") // automatically infer data types
      .load(trainingDataPath)

    // Convert label to double from int
    val toDouble = org.apache.spark.sql.functions.udf[Double, Int](intLabel => intLabel.asInstanceOf[Double])
    var df = dfInitial.withColumn("QuoteConversion_Flag", toDouble(dfInitial("QuoteConversion_Flag")))

    val indexer = new StringIndexer()
      .setInputCol("QuoteConversion_Flag")
      .setOutputCol("label")

    // Split into training and testing data
    val Array(train1, train2, train3, train4, train5, train6, train7, testing) = df.randomSplit(Array(0.2, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2))

    // Evaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("QuoteConversion_Flag")
      .setPredictionCol("prediction")
      .setMetricName("precision")

    // Cast all int fields to double fields
    val numericFields = df.schema.filter(
      entry => entry.dataType == IntegerType ||
        entry.dataType == DoubleType
    )
    df.schema.foreach{ (entry) => {
      entry.dataType match {
        case IntegerType => {
          df = df.withColumn(entry.name, toDouble(df(entry.name)))
        }
        case _ => {}
      }
    }}

    // Create new pipeline
    val stringFields = df.schema.filter(entry => entry.dataType == StringType &&
      (entry.name == "Field6" ||
        entry.name == "Field12" ||
        entry.name == "CoverageField8" ||
        entry.name == "CoverageField9"))
    val indexers: Array[PipelineStage] = stringFields.toArray.map( (entry) =>
      new StringIndexer()
        .setInputCol(entry.name)
        .setOutputCol(entry.name + "_index")
    )
    var encoders: Array[PipelineStage] = stringFields.toArray.map( (entry) =>
      new OneHotEncoder()
        .setInputCol(entry.name + "_index")
        .setOutputCol(entry.name + "_vec")
    )
    val assembler = new VectorAssembler()
      .setInputCols(numericFields.map(_.name).toArray ++ stringFields.map(_.name + "_vec"))
      .setOutputCol("features")
    val rf = new WahooRandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

    val stages = (indexers :+ indexer) ++ encoders :+ assembler :+ rf
    val pipeline = new Pipeline().setStages(stages)

    // Model 1 - trained on first batch of data
    val forest = pipeline.fit(train1)
    var predictions = forest.transform(testing)
    var accuracy = evaluator.evaluate(predictions)
    println("test error after being trained on data batch 1: " + (1.0 - accuracy))

//     // Model 2 - trained using more trees
//     rf.setNumTrees(100)
//     predictions = forest.transform(testing)
//     accuracy = evaluator.evaluate(predictions)
//     println("test error after adding more trees: " + (1.0 - accuracy))
//
//     // Model 3 - trained on first and second batches of data
//     rf.setNumTrees(10)
//     val train12: DataFrame = train1.unionAll(train2)
//     predictions = forest.transform(train12)
//     accuracy = evaluator.evaluate(predictions)
//     println("test error after being trained on data batches 1 and 2: " + (1.0 - accuracy))
//
//     // Model 4 - trained on data batches 1-3
//     val train123: DataFrame = train12.unionAll(train3)
//     predictions = forest.transform(train123)
//     accuracy = evaluator.evaluate(predictions)
//     println("test error after being trained on data batches 1-3: " + (1.0 - accuracy))
//
//     // Model 5 - trained on data batches 1-4
//     val train1234: DataFrame = train123.unionAll(train4)
//     predictions = forest.transform(train1234)
//     accuracy = evaluator.evaluate(predictions)
//     println("test error after being trained on data batches 1-4: " + (1.0 - accuracy))
//
//     // Model 6 - trained on data batches 1-5
//     val train12345: DataFrame = train1234.unionAll(train5)
//     predictions = forest.transform(train12345)
//     accuracy = evaluator.evaluate(predictions)
//     println("test error after being trained on data batches 1-5: " + (1.0 - accuracy))
//
//     // Model 7 - trained on data batches 1-6
//     val train123456: DataFrame = train12345.unionAll(train6)
//     predictions = forest.transform(train123456)
//     accuracy = evaluator.evaluate(predictions)
//     println("test error after being trained on data batches 1-6: " + (1.0 - accuracy))
//
//     // Model 8 - trained on data batches 1-7
//     val train1234567: DataFrame = train123456.unionAll(train7)
//     predictions = forest.transform(train1234567)
//     accuracy = evaluator.evaluate(predictions)
    // println("test error after being trained on data batches 1-7: " + (1.0 - accuracy))
  }
}
