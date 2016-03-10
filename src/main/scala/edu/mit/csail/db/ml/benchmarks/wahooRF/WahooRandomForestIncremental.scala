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

    val numBatches = 10
    val batches = generateBatches(numBatches, df)
    runAllBenchmarks(rf, evaluator, batches, numBatches, 3, 2, sc, sqlContext, true, false)
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
//    println("batched strategy")
//    rf.wahooStrategy = new WahooStrategy(erf, BatchedStrategy)
//    runBenchmark(rf, evaluator, batches, numBatches,
//      initialDepth, incrementParam, sc, sqlContext, predictive)
    println("random replacement strategy")
    rf.wahooStrategy = new WahooStrategy(erf, RandomReplacementStrategy)
    runBenchmark(rf, evaluator, batches, numBatches,
      initialDepth, incrementParam, sc, sqlContext, predictive)
    println("control")
    rf.wahooStrategy = new WahooStrategy(erf, DefaultStrategy)
    runBenchmark(rf, evaluator, batches, numBatches,
      initialDepth, 0, sc, sqlContext, predictive)
  }

  def runBenchmark(rf: RandomForestClassifier,
                   evaluator: MulticlassClassificationEvaluator,
                   batches: Array[DataFrame],
									 numBatches: Int,
									 initialDepth: Int,
									 incrementParam: Int,
									 sc: SparkContext,
									 sqlContext: SQLContext,
                   predictive: Boolean) {
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
    // model._trees.foreach(tree => { //   println("tree " + (1.0 - evaluator.evaluate(tree.transform(batches.last))))
    // })

    println(numPoints)
    println(time)
    println((1.0 - accuracy))
		var currDepth = initialDepth + incrementParam
    var currDF = batches(0)
    Range(1,numBatches).map { batch => {
      timer.start("training " + batch)
      if (rf.wahooStrategy == RandomReplacementStrategy) {
        val weightAdjustments = model._trees.map(tree => {
          val prediction = tree.transform(batches(batch))
          evaluator.evaluate(prediction)
        })
        val maxAdjustment = weightAdjustments.max
        val normalizedAdjustments = weightAdjustments.map(_/maxAdjustment)
        model._trees.zipWithIndex.foreach{ case (tree, index) => {
          model.weights(index) *= normalizedAdjustments(index)
        }}
      }



      if (rf.wahooStrategy.isIncremental) {
			  rf.setMaxDepth(currDepth)
      }
      numPoints += batches(batch).count()
			model = if (rf.wahooStrategy == DefaultStrategy) {
        currDF = currDF.unionAll(batches(batch))
        rf.fit(currDF)
      } else {
      	rf.update(model, batches(batch))
      }
      // modelUpdated._trees.foreach(tree => {
      //   println("tree " + (1.0 - evaluator.evaluate(tree.transform(batches.last))))
      // })

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
      if (rf.wahooStrategy.isIncremental) {
			  currDepth += incrementParam
      }
    }}
  }
}
