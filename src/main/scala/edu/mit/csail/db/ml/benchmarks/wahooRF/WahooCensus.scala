package org.apache.spark.ml.wahoo

import edu.mit.csail.db.ml.benchmarks.wahoo.WahooUtils
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.tree.impl.TimeTracker
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by kathrynsiegel on 2/15/16.
  */
object WahooCensus {
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
    val indexer = WahooUtils.createStringIndexer("income", "label")
    val evaluator = WahooUtils.createEvaluator("income", "prediction")
    val numericFields = WahooUtils.getNumericFields(df, "income")
    val stringFields: Seq[StructField] = WahooUtils.getStringFields(df,
      Some(Array("workclass", "education", "marital-status", "occupation", "relationship", "race", "sex", "native-country")))
    val stringProcesser = WahooUtils.processStringColumns(df, stringFields)
    val assembler = WahooUtils.createAssembler(numericFields.map(_.name).toArray ++ stringFields.map(_.name + "_vec"))
    df = WahooUtils.processDataFrame(df, stringProcesser :+ indexer :+ assembler)

    val rf: RandomForestClassifier = new WahooRandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(1)

    println("batched strategy")
    rf.wahooStrategy = new WahooStrategy(false, BatchedStrategy)
    WahooRandomForestIncremental.runBenchmark(rf, df, evaluator, 10.0, 5, 5, 1, sc, sqlContext)
    // println("online strategy")
    // rf.wahooStrategy = new WahooStrategy(false, OnlineStrategy)
    // runBenchmark(rf, df, evaluator, 8.0)
    // println("random replacement strategy")
    // rf.wahooStrategy = new WahooStrategy(false, RandomReplacementStrategy)
    // runBenchmark(rf, df, evaluator, 8.0)
    // println("control")
    // runControlBenchmark(rf, df, evaluator, 8.0)
  }

//   def runBenchmark(rf: RandomForestClassifier, df: DataFrame,
//                    evaluator: MulticlassClassificationEvaluator,
//                    batchSizeConstant: Double,
// 									 numBatches: Int,
// 									 initialDepth: Int,
// 									 incrementParam: Int) {
// 		rf.setMaxDepth(initialDepth)
//     val batchWeights: ArrayBuffer[Double] = new ArrayBuffer[Double]()
//     Range(0, numBatches).map { i =>
//       batchWeights += (1.0/batchSizeConstant)
//     }
//     batchWeights += 2.0/batchSizeConstant
// 
//     val batches = df.randomSplit(batchWeights.toArray)
//     val timer = new TimeTracker()
//     var numPoints = batches(0).count()
//     timer.start("training 0")
//     val model: RandomForestClassificationModel = rf.fit(batches(0))
//     var time = timer.stop("training 0")
//     var predictions = model.transform(batches(1))
//     var accuracy = evaluator.evaluate(predictions)
//     println("time: " + time)
//     println("error: " + (1.0 - accuracy))
// 		var currDepth = initialDepth + incrementParam
//     Range(1,numBatches).map { batch => {
// 			rf.setMaxDepth(currDepth)
//       numPoints += batches(batch).count()
//       timer.start("training " + batch)
//       val modelUpdated = rf.update(model, batches(batch))
//       time = timer.stop("training " + batch)
//       predictions = modelUpdated.transform(batches.last)
//       accuracy = evaluator.evaluate(predictions)
//       println("time: " + time)
//       println("error: " + (1.0 - accuracy))
// 			currDepth += incrementParam
//     }}
//   }
// 
// 
//   def runControlBenchmark(rf: RandomForestClassifier, df: DataFrame,
//                    evaluator: MulticlassClassificationEvaluator,
//                    batchSizeConstant: Double) {
//     val numBatches = 6
//     val batchWeights: ArrayBuffer[Double] = new ArrayBuffer[Double]()
//     Range(0, numBatches).map { i =>
//       batchWeights += (1.0/batchSizeConstant)
//     }
//     batchWeights += 0.2
//     val batches = df.randomSplit(batchWeights.toArray)
//     val timer = new TimeTracker()
//     var currDF = batches(0)
//     var numPoints = currDF.count()
//     println(numPoints)
//     timer.start("training 0")
//     val model: RandomForestClassificationModel = rf.fit(currDF)
//     var time = timer.stop("training 0")
//     var predictions = model.transform(batches.last)
//     var accuracy = evaluator.evaluate(predictions)
//     println(time)
//     println(1.0 - accuracy)
//     Range(1,numBatches).map { batch => {
//       numPoints += batches(batch).count()
//       println(numPoints)
//       currDF = currDF.unionAll(batches(batch))
//       timer.start("training " + batch)
//       val modelUpdated = rf.fit(currDF)
//       time = timer.stop("training " + batch)
//       predictions = modelUpdated.transform(batches.last)
//       accuracy = evaluator.evaluate(predictions)
//       println(time)
//       println(1.0 - accuracy)
//     }}
//   }
}
