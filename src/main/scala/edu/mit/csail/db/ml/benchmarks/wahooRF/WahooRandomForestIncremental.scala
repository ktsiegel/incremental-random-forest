package org.apache.spark.ml.wahoo

import edu.mit.csail.db.ml.benchmarks.wahoo.WahooUtils
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.impl.TimeTracker
import org.apache.spark.sql.{DataFrame, SQLContext}
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
    val sqlContext = new SQLContext(sc)
    var df: DataFrame = WahooUtils.readData(trainingDataPath, sqlContext)
    df = WahooUtils.processIntColumns(df)
    val indexer = WahooUtils.createStringIndexer("QuoteConversion_Flag", "label")
    val evaluator = WahooUtils.createEvaluator("QuoteConversion_Flag", "prediction")
    val numericFields = WahooUtils.getNumericFields(df, "QuoteConversion_Flag")
    val stringFields: Seq[StructField] = WahooUtils.getStringFields(df,
      Some(Array("Field6", "Field12", "CoverageField8", "CoverageField9")))
    val stringProcesser = WahooUtils.processStringColumns(df, stringFields)
    val assembler = WahooUtils.createAssembler(numericFields.map(_.name).toArray ++ stringFields.map(_.name + "_vec"))
    df = WahooUtils.processDataFrame(df, stringProcesser :+ indexer :+ assembler)

    val rf: RandomForestClassifier = new WahooRandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)

    val numBatches = 100
    val batches = generateBatches(numBatches, df)
    runAllBenchmarks(rf, evaluator, batches, numBatches, 10, 1, sc, sqlContext, true, false)
  }

  def run(trainingDataPath: String, destinationField: String): Unit = {
    val conf = new SparkConf()
      .setAppName("Wahoo")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    var df: DataFrame = WahooUtils.readData(trainingDataPath, sqlContext)
    df = WahooUtils.processIntColumns(df)
    val indexer = WahooUtils.createStringIndexer(destinationField, "label")
    val evaluator = WahooUtils.createEvaluator(destinationField, "prediction")
    val numericFields = WahooUtils.getNumericFields(df, destinationField)
    val assembler = WahooUtils.createAssembler(numericFields.map(_.name).toArray)
    df = WahooUtils.processDataFrame(df, Array(indexer, assembler))

    val rf: RandomForestClassifier = new WahooRandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)

    val numBatches = 10
    val batches = generateBatches(numBatches, df)
    runAllBenchmarks(rf, evaluator, batches, numBatches, 10, 1, sc, sqlContext, true, false)
  }

  def generateBatches(numBatches: Int, df: DataFrame): Array[DataFrame] = {
    val batchWeights: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    var totalWeight: Double = 0.0
    Range(0, numBatches).map { i =>
      val coeff: Double = 1.0
      batchWeights += coeff
      totalWeight += coeff
    }
    batchWeights += (totalWeight / 4.0)
    df.randomSplit(batchWeights.toArray)
  }

  def runAllBenchmarks(rf: RandomForestClassifier,
                       evaluator: MulticlassClassificationEvaluator,
                       batches: Array[DataFrame],
                       numBatches: Int,
                       initialDepth: Int,
                       incrementParam: Int,
                       sc: SparkContext,
                       sqlContext: SQLContext,
                       predictive: Boolean,
                       erf: Boolean) {
    rf.wahooStrategy = new WahooStrategy(false, CombinedStrategy)
    val regrowProps = Array(0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1)
    val incrementalProps = Array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1)
    regrowProps.foreach(regrowProp => {
      incrementalProps.foreach(incrementalProp => {
        rf.regrowProp = regrowProp
        rf.incrementalProp = incrementalProp
        println("benchmark: regrow " + regrowProp + ", increment " + incrementalProp)
        rf.setInitialMaxDepth(initialDepth)
        val timer = new TimeTracker()
        var numPoints = batches(0).count()
        timer.start("training 0")
        var model = rf.fit(batches(0))
        var time = timer.stop("training 0")
        var predictions = if (predictive) {
          model.transform(batches(1))
        } else {
          model.transform(batches.last)
        }
        var accuracy = evaluator.evaluate(predictions)
        println(numPoints)
        println(time)
        println((1.0 - accuracy))

        Range(1,numBatches).map { batch => {
          numPoints += batches(batch).count()
          timer.start("training " + batch)
          model = rf.update(model, batches(batch))
          time = timer.stop("training " + batch)
          predictions = if (predictive) {
            model.transform(batches(batch+1))
          } else {
            model.transform(batches.last)
          }
          accuracy = evaluator.evaluate(predictions)
          println(numPoints)
          println(time)
          println((1.0 - accuracy))
        }}
      })
    })


    runControlBenchmark(evaluator,batches,numBatches,initialDepth,incrementParam,
      sc,sqlContext,predictive)
  }

  def runControlBenchmark(evaluator: MulticlassClassificationEvaluator,
                   batches: Array[DataFrame],
									 numBatches: Int,
									 initialDepth: Int,
									 incrementParam: Int,
									 sc: SparkContext,
									 sqlContext: SQLContext,
                   predictive: Boolean) {
    println("benchmark: control")
    val rf: org.apache.spark.ml.classification.RandomForestClassifier =
      new org.apache.spark.ml.classification.RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

		rf.setMaxDepth(initialDepth)
    val timer = new TimeTracker()
    var numPoints = batches(0).count()
    timer.start("training 0")
    var model = rf.fit(batches(0))
    var time = timer.stop("training 0")
    var predictions = if (predictive) {
      model.transform(batches(1))
    } else {
      model.transform(batches.last)
    }
    var accuracy = evaluator.evaluate(predictions)

    println(numPoints)
    println(time)
    println((1.0 - accuracy))
		var currDepth = initialDepth + incrementParam
    var currDF = batches(0)
    Range(1,numBatches).map { batch =>
      numPoints += batches(batch).count()

      timer.start("training " + batch)
      currDF = currDF.unionAll(batches(batch))
      model = rf.fit(currDF)

      time = timer.stop("training " + batch)
      predictions = if (predictive) {
        model.transform(batches(batch+1))
      } else {
        model.transform(batches.last)
      }
      accuracy = evaluator.evaluate(predictions)
      println(numPoints)
      println(time)
      println((1.0 - accuracy))
    }
  }
}
