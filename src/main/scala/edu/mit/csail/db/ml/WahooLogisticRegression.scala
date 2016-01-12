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
import org.apache.spark.ml.{HasModelDb, CanCache, LogisticRegressionSpec, WahooContext, MultiThreadTrain, WahooLog}

/**
 * A smarter logistic regression which caches old models in the model database and looks them up before
 * trying to retrain. Make sure to to call the setDb method to give it a model database.
 */
class WahooLogisticRegression(uid: String, wc: Option[WahooContext]) extends LogisticRegression(uid)
with HasModelDb with CanCache[LogisticRegressionModel]
with MultiThreadTrain[LogisticRegressionModel] {
  if (wc.isDefined) this.setDb(wc.get.modelDB)

  override def train(dataset: DataFrame): LogisticRegressionModel = {
    val log = new WahooLog(wc)
    log.addMessage(s"Running model $uid")
    val ms = modelSpec(dataset).toString
    log.addMessage(s"ModelSpec: $ms")
    val model = super.train(dataset)
    log.addMessage(s"Training complete")
    val summary = model.summary
    val numIter = summary.totalIterations
    val objhist = summary.objectiveHistory.mkString("[", ", ", "]")
    log.addMessage(s"Objective History: $objhist")
    log.addMessage(s"# Iterations: $numIter")
    log.addMessage(s"Finished model $uid")
    this.modelLogs += (model.uid -> log)
    model
  }

  def this(wc: Option[WahooContext]) = this(Identifiable.randomUID("logreg"), wc)
  def this(uid: String) = this(uid, None)
  override def modelSpec(dataset: DataFrame): LogisticRegressionSpec =
    new LogisticRegressionSpec(dataset.columns, super.getRegParam, super.getMaxIter)

  /**
   * Trains a model using a warm start. 
   * @param oldModel - the trained model that will be trained further
   * @param dataset - the dataset on which the model will be trained
   * @param addedIterations - the additional iterations on which the model
   *                          will be trained
   * @return a trained model
   */
  def train(oldModel: LogisticRegressionModel, dataset: DataFrame, addedIterations: Int): LogisticRegressionModel = {
    val instances = getInstances(dataset)
    val (summarizer, labelSummarizer) = getSummarizers(instances)
    val initialCoefficients = Vectors.dense(
      if ($(fitIntercept)) oldModel.weights.toArray :+ oldModel.intercept
      else oldModel.weights.toArray)
    runRegression(dataset, instances, summarizer, labelSummarizer, initialCoefficients)
  }
}

object WahooLogisticRegression extends DefaultParamsReadable[WahooLogisticRegression] {
  override def load(path: String): WahooLogisticRegression = super.load(path)
}

