package org.apache.spark.ml.wahoo

import edu.mit.csail.db.ml.benchmarks.wahoo.WahooUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.impl.TimeTracker
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

object RandomForestControl {
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
//    println("Very large initial base compared to update batches (100:1)")
//    val a100: ArrayBuffer[Double] = new ArrayBuffer[Double]() += (100.0/150.0)
//    Range(0, 20).map { i =>
//      a100 += (1.0/150.0)
//    }
//    a100 += 30.0/150.0
//    val train100: Array[DataFrame] = df.randomSplit(a100.toArray)
//    var timer = new TimeTracker()
//    var currDF100 = train100(0)
//    Range(0,21).map { batch => {
//      val batch = 0
//      if (batch > 0) {
//        currDF100 = currDF100.join(train100(batch))
//      }
//      timer.start("training " + batch.toString)
//      val model: RandomForestClassificationModel = rf.fit(currDF100)
//      val time = timer.stop("training " + batch.toString)
//      val predictions = model.transform(train100.last)
//      val accuracy = evaluator.evaluate(predictions)
//      println(time)
//      println(1.0 - accuracy)
//    }}

//    println("Large initial base compared to update batches (50:1)")
//    val a50: ArrayBuffer[Double] = new ArrayBuffer[Double]() += (50.0/88.0)
//    Range(0, 20).map { i =>
//      a50 += (1.0/88.0)
//    }
//    a50 += 18.0/88.0
//    val train50: Array[DataFrame] = df.randomSplit(a50.toArray)
//    timer = new TimeTracker()
//    var currDF50 = train50(0)
//    Range(0,21).map { batch => {
//      val batch = 0
//      if (batch > 0) {
//        currDF50 = currDF50.join(train50(batch))
//      }
//      timer.start("training " + batch.toString)
//      val model: RandomForestClassificationModel = rf.fit(currDF50)
//      val time = timer.stop("training " + batch.toString)
//      val predictions = model.transform(train50.last)
//      val accuracy = evaluator.evaluate(predictions)
//      println(time)
//      println(1.0 - accuracy)
//    }}
//
//    println("Medium initial base compared to update batches (25:1)")
//    val a25: ArrayBuffer[Double] = new ArrayBuffer[Double]() += (100.0/150.0)
//    Range(0, 20).map { i =>
//      a25 += (1.0/150.0)
//    }
//    a25 += 30.0/150.0
//    val train25: Array[DataFrame] = df.randomSplit(a25.toArray)
//    timer = new TimeTracker()
//    var currDF25 = train25(0)
//    Range(0,21).map { batch => {
//      val batch = 0
//      if (batch > 0) {
//        currDF25 = currDF25.join(train25(batch))
//      }
//      timer.start("training " + batch.toString)
//      val model: RandomForestClassificationModel = rf.fit(currDF25)
//      val time = timer.stop("training " + batch.toString)
//      val predictions = model.transform(train25.last)
//      val accuracy = evaluator.evaluate(predictions)
//      println(time)
//      println(1.0 - accuracy)
//    }}


    println("Small initial base compared to update batches (2:1)")
    val numBatches = 10
    val a2: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    Range(0, numBatches).map { i =>
      a2 += (1.0/10000.0)
    }
    a2 += 20.0/100.0
    val train2 = df.randomSplit(a2.toArray)
    var timer = new TimeTracker()
    var currDF2 = train2(0)
    Range(0,numBatches).map { batch => {
      println("batch: " + batch)
      if (batch > 0) {
        currDF2 = currDF2.unionAll(train2(batch))
      }
      println("count: " + currDF2.count())
      timer.start("training " + batch.toString)
      val model: RandomForestClassificationModel = rf.fit(currDF2)
      val time = timer.stop("training " + batch.toString)
      val predictions = model.transform(train2.last)
      val accuracy = evaluator.evaluate(predictions)
      println(time)
      println(1.0 - accuracy)
    }}
  }
}
              
