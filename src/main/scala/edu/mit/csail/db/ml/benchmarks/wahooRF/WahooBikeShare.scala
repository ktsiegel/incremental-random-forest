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
object WahooBikeShare {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Wahoo")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val numBatches = 25
    val indexer = WahooUtils.createStringIndexer("bikeCountIndicator", "label")
    val binarizer = WahooUtils.makeBinarizer("cnt", "bikeCountIndicator", 120)

    val batches: Array[DataFrame] = Range(1,numBatches+2).map { index => {
      val trainingDataPath = "kaggleData/bikeshare/bikeshare" + index.toString + ".csv"
      var df: DataFrame = WahooUtils.readData(trainingDataPath, sqlContext)
      df = WahooUtils.processIntColumns(df)
      df = WahooUtils.processStringColumnsAsInt(df)
      df = binarizer.transform(df)
      val numericFields = WahooUtils.getNumericFields(df, Array("cnt", "bikeCountIndicator"))
      val assembler = WahooUtils.createAssembler(numericFields.map(_.name).toArray)
      val processStages: Array[PipelineStage] = Array(indexer, assembler)
      WahooUtils.processDataFrame(df, processStages)
    }}.toArray

    val evaluator = WahooUtils.createEvaluator("bikeCountIndicator", "prediction")
    val rf: RandomForestClassifier = new WahooRandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

    WahooRandomForestIncremental.runAllBenchmarks(rf, evaluator, batches,
      numBatches, 10, 1, sc, sqlContext, true, false)
  }
}
