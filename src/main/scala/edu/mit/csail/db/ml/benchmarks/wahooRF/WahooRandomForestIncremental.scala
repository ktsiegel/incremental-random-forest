package org.apache.spark.ml.wahoo

import edu.mit.csail.db.ml.benchmarks.wahoo.WahooUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.impl.TimeTracker
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

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
    val conf = new SparkConf()
      .setAppName("Wahoo")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    var df: DataFrame = WahooUtils.readData(trainingDataPath, sc)
    df = WahooUtils.processIntColumns(df)
    val indexer = WahooUtils.createStringIndexer("QuoteConversion_Flag", "label")
    val evaluator = WahooUtils.createEvaluator("QuoteConversion_Flag", "prediction")
    val numericFields = WahooUtils.getNumericFields(df)
    val stringFields: Seq[StructField] = WahooUtils.getStringFields(df,
        Some(Array("Field6", "Field12", "CoverageField8", "CoverageField9")))
    val stringProcesser = WahooUtils.processStringColumns(df, stringFields)
    val assembler = WahooUtils.createAssembler(numericFields.map(_.name).toArray ++ stringFields.map(_.name + "_vec"))
    df = WahooUtils.processDataFrame(df, stringProcesser :+ indexer :+ assembler)

    val rf = new WahooRandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

    rf.wahooStrategy = new WahooStrategy(false, OnlineStrategy)
    // rf.wahooStrategy = new WahooStrategy(false, RandomReplacementStrategy)
    rf.sc = Some(sc)

    println("Small initial base compared to update batches (1:1)")
    val numBatches = 10
    val a2: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    Range(0, numBatches).map { i =>
      a2 += (1.0/10000.0)
    }
    a2 += 20.0/100.0
    val train2 = df.randomSplit(a2.toArray)
    var timer = new TimeTracker()
    var currDF2 = train2(0)
    var numPoints = currDF2.count()
    println(numPoints)
    timer.start("training 0")
    val model: RandomForestClassificationModel = rf.fit(currDF2)
    var time = timer.stop("training 0")
    var predictions = model.transform(train2.last)
    var accuracy = evaluator.evaluate(predictions)
    println(time)
    println(1.0 - accuracy)
    Range(1,numBatches).map { batch => {
      numPoints += train2(batch).count()
      println(numPoints)
      timer.start("training " + batch)
      val modelUpdated = rf.update(model, train2(batch))
      time = timer.stop("training " + batch)
      predictions = modelUpdated.transform(train2.last)
      accuracy = evaluator.evaluate(predictions)
      println(time)
      println(1.0 - accuracy)
    }}
  }
}
