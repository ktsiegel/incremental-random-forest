package edu.mit.csail.db.ml.benchmarks.wahoo

import org.apache.spark.ml.wahoo.{RandomForestClassificationModel, WahooRandomForestClassifier}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SQLContext}

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.{Estimator, Transformer, PipelineStage}
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

    var df: DataFrame = WahooUtils.readData(trainingDataPath, "Wahoo", "local[2]")
    val indexer = WahooUtils.createStringIndexer("QuoteConversion_Flag", "label")
    val evaluator = WahooUtils.createEvaluator("QuoteConversion_Flag", "prediction")
    df = WahooUtils.columnsIntToDouble(df)

    val numericFields = df.schema.filter(
      entry => entry.dataType == IntegerType ||
        entry.dataType == DoubleType
    )

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
    val encoders: Array[PipelineStage] = stringFields.toArray.map( (entry) =>
      new OneHotEncoder()
        .setInputCol(entry.name + "_index")
        .setOutputCol(entry.name + "_vec")
    )
    val assembler = new VectorAssembler()
      .setInputCols(numericFields.map(_.name).toArray) // ++ stringFields.map(_.name + "_vec"))
      .setOutputCol("features")
    val rf = new WahooRandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(5)

    val stages = (indexers :+ indexer) ++ encoders :+ assembler
    stages.foreach(stage =>
      stage match {
        case estimator: Estimator[_] =>
          df = estimator.fit(df).transform(df)
        case transformer: Transformer =>
          df = transformer.transform(df)
    })

    // Split into training and testing data
    val Array(train1, train2, train3, train4, train5, train6, train7, testing) = df.randomSplit(Array(0.2, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2))

    // Model 1 - trained on first batch of data
    val model1: RandomForestClassificationModel = rf.fit(train1)
    var predictions = model1.transform(testing)
    var accuracy = evaluator.evaluate(predictions)
    println("test error after being trained on data batch 1: " + (1.0 - accuracy))

    // Model 2 - trained using more trees
    val model2 = rf.addTrees(model1, train1, 10)
    predictions = model2.transform(testing)
    accuracy = evaluator.evaluate(predictions)
    println("test error after adding more trees: " + (1.0 - accuracy))


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
