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
      .setNumTrees(10)

    val batchSizeConstant = 1000.0
    val batchWeights: ArrayBuffer[Double] = new ArrayBuffer[Double]()
    Range(0, 1000).map { i =>
      batchWeights += (1.0 / batchSizeConstant)
    }
    batchWeights += 2.0 / batchSizeConstant

    val batches = df.randomSplit(batchWeights.toArray)

    runAllBenchmarks(rf, df, evaluator, batches, 10, 10, 2, sc, sqlContext)
  }

  def runAllBenchmarks(rf: RandomForestClassifier, df: DataFrame,
                       evaluator: MulticlassClassificationEvaluator,
                       batches: Array[DataFrame],
                       numBatches: Int,
                       initialDepth: Int,
                       incrementParam: Int,
                       sc: SparkContext,
                       sqlContext: SQLContext) {
    println("batched strategy")
    rf.wahooStrategy = new WahooStrategy(false, BatchedStrategy)
    runBenchmark(rf, df, evaluator, batches, numBatches,
      initialDepth, incrementParam, sc, sqlContext)
    println("online strategy")
    rf.wahooStrategy = new WahooStrategy(false, OnlineStrategy)
    runBenchmark(rf, df, evaluator, batches, numBatches,
      initialDepth, incrementParam, sc, sqlContext)
    println("random replacement strategy")
    rf.wahooStrategy = new WahooStrategy(false, RandomReplacementStrategy)
    runBenchmark(rf, df, evaluator, batches, numBatches,
      initialDepth, incrementParam, sc, sqlContext)
    println("control")
    rf.wahooStrategy = new WahooStrategy(false, DefaultStrategy)
    runBenchmark(rf, df, evaluator, batches, numBatches,
      initialDepth, 0, sc, sqlContext)
  }

  def runBenchmark(rf: RandomForestClassifier, df: DataFrame,
                   evaluator: MulticlassClassificationEvaluator,
                   batches: Array[DataFrame],
									 numBatches: Int,
									 initialDepth: Int,
									 incrementParam: Int,
									 sc: SparkContext,
									 sqlContext: SQLContext) {
		rf.setMaxDepth(initialDepth)
    val timer = new TimeTracker()
    var numPoints = batches(0).count()
    timer.start("training 0")
    val model = rf.fit(batches(0))
    var time = timer.stop("training 0")
    var predictions = model.transform(batches.last)
    var accuracy = evaluator.evaluate(predictions)
    println("num points: " + numPoints)
    println("time: " + time)
    println("accuracy: " + (1.0 - accuracy))
		var currDepth = initialDepth + incrementParam
    var currDF = batches(0)
    Range(1,numBatches).map { batch => {
      if (rf.wahooStrategy.isIncremental) {
			  rf.setMaxDepth(currDepth)
      }
      numPoints += batches(batch).count()
      timer.start("training " + batch)
			val modelUpdated = if (rf.wahooStrategy == OnlineStrategy) {
        var tempModel = model
        batches(batch).foreach(point => {
          val pointDataset = sc.parallelize(Array(point))
          val pDF = sqlContext.createDataFrame(pointDataset, df.schema)
          tempModel = rf.update(model, pDF)
        })
        tempModel
      } else if (rf.wahooStrategy == DefaultStrategy) {
        currDF = currDF.unionAll(batches(batch))
        rf.fit(currDF)
      } else {
      	rf.update(model, batches(batch))
      }
      time = timer.stop("training " + batch)
      predictions = modelUpdated.transform(batches.last)
      accuracy = evaluator.evaluate(predictions)
      println("num points: " + numPoints)
      println("time: " + time)
      println("error: " + (1.0 - accuracy))
      if (rf.wahooStrategy.isIncremental) {
			  currDepth += incrementParam
      }
    }}
  }
}
