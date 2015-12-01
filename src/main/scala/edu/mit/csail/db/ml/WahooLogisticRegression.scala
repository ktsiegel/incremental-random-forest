package org.apache.spark.ml.classification

import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.util._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.ml.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.{HasModelDb, CanCache, LogisticRegressionSpec, WahooContext}


/**
 * Wahoo logistic regression.
 * Currently, this class only supports binary classification.
 * 
 * It is a smarter logistic regression which caches old models in the model database and looks them up before
 * trying to retrain. Make sure to to call the setDb method to give it a model database.
 */
class WahooLogisticRegression(override val uid: String) extends LogisticRegression(uid)
  with HasModelDb
  with CanCache[LogisticRegressionModel] {

  var wc: WahooContext = _

  def this(wcontext: WahooContext) = {
    this(Identifiable.randomUID("logreg"))
    wc = wcontext
    this.setDb(wc.modelDB)
  }

  override def modelSpec(dataset: DataFrame): LogisticRegressionSpec =
    new LogisticRegressionSpec(dataset.columns, super.getRegParam, super.getMaxIter)

  override protected def train(dataset: DataFrame): LogisticRegressionModel = {
    wc.log_msg(s"Running model $uid")
    val ms = modelSpec(dataset).toString
    wc.log_msg(s"ModelSpec: $ms")
    val model = super.train(dataset)
    wc.log_msg(s"Training complete")
    val summary = model.summary
    val numIter = summary.totalIterations
    val objhist = summary.objectiveHistory.mkString("[", ", ", "]")
    wc.log_msg(s"Objective History: $objhist")
    wc.log_msg(s"# Iterations: $numIter")
    wc.log_msg(s"Finished model $uid")
    model
  }

  def train(oldModel: LogisticRegressionModel, dataset: DataFrame, addedIterations: Int): LogisticRegressionModel = {
    val (instances, summarizer, labelSummarizer) = super.startTrain(dataset)
    val initialCoefficientsWithIntercept = Vectors.dense(
      if ($(fitIntercept)) oldModel.weights.toArray :+ oldModel.intercept
      else oldModel.weights.toArray)
    super.finishTrain(dataset, instances, summarizer, labelSummarizer, initialCoefficientsWithIntercept)
  }
}

object WahooLogisticRegression extends DefaultParamsReadable[WahooLogisticRegression] {
  override def load(path: String): WahooLogisticRegression = super.load(path)
}

