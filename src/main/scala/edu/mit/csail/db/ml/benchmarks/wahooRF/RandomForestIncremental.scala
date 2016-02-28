package org.apache.spark.ml.wahoo

import edu.mit.csail.db.ml.benchmarks.wahoo.WahooUtils
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

    var df: DataFrame = WahooUtils.readData(trainingDataPath, "Wahoo", "local[2]")
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
      .setNumTrees(5)

    println("Very large initial base compared to update batches (100:1)")
    val a100: ArrayBuffer[Double] = new ArrayBuffer[Double]() += (100.0/150.0)
    Range(0, 20).map { i =>
      a100 += (1.0/150.0)
    }
    a100 += 30.0/150.0
    val train100: Array[DataFrame] = df.randomSplit(a100.toArray)
    var timer = new TimeTracker()
    var currDF100 = train100(0)
    Range(0,21).map { batch => {
      val batch = 0
      if (batch > 0) {
        currDF100 = currDF100.join(train100(batch))
      }
      timer.start("training " + batch.toString)
      val model: RandomForestClassificationModel = rf.fit(currDF100)
      val time = timer.stop("training " + batch.toString)
      val predictions = model.transform(train100.last)
      val accuracy = evaluator.evaluate(predictions)
      println(time)
      println(1.0 - accuracy)
    }}

    println("Large initial base compared to update batches (50:1)")
    val a50: ArrayBuffer[Double] = new ArrayBuffer[Double]() += (100.0/150.0)
    Range(0, 20).map { i =>
      a50 += (1.0/150.0)
    }
    a50 += 30.0/150.0
    val train50: Array[DataFrame] = df.randomSplit(a50.toArray)
    timer = new TimeTracker()
    var currDF50 = train50(0)
    Range(0,21).map { batch => {
      val batch = 0
      if (batch > 0) {
        currDF50 = currDF50.join(train50(batch))
      }
      timer.start("training " + batch.toString)
      val model: RandomForestClassificationModel = rf.fit(currDF50)
      val time = timer.stop("training " + batch.toString)
      val predictions = model.transform(train50.last)
      val accuracy = evaluator.evaluate(predictions)
      println(time)
      println(1.0 - accuracy)
    }}
  }
}
              
