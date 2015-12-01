package org.apache.spark.ml.classification

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.{Since, Experimental}
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
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
 * :: Experimental ::
 * Wahoo logistic regression.
 * Currently, this class only supports binary classification.
 * 
 * It is a smarter logistic regression which caches old models in the model database and looks them up before
 * trying to retrain. Make sure to to call the setDb method to give it a model database.
 */
@Experimental
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
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val w = if ($(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] = dataset.select(col($(labelCol)), w, col($(featuresCol))).map {
      case Row(label: Double, weight: Double, features: Vector) =>
        Instance(label, weight, features)
    }

    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val (summarizer, labelSummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        instance: Instance) =>
        (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
        (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        new MultivariateOnlineSummarizer, new MultiClassSummarizer)(seqOp, combOp)
    }

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numClasses = histogram.length
    val numFeatures = summarizer.mean.size

    if (numInvalid != 0) {
      val msg = s"Classification labels should be in {0 to ${numClasses - 1} " +
      s"Found $numInvalid invalid labels."
      logError(msg)
      throw new SparkException(msg)
    }

    if (numClasses > 2) {
      val msg = s"Currently, LogisticRegression with ElasticNet in ML package only supports " +
      s"binary classification. Found $numClasses in the input dataset."
      logError(msg)
      throw new SparkException(msg)
    }

    val featuresMean = summarizer.mean.toArray
    val featuresStd = summarizer.variance.toArray.map(math.sqrt)

    val regParamL1 = $(elasticNetParam) * $(regParam)
    val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

    val costFun = new LogisticCostFun(instances, numClasses, $(fitIntercept), $(standardization),
      featuresStd, featuresMean, regParamL2)

    val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
      new BreezeLBFGS[BDV[Double]](addedIterations, 10, $(tol))
    } else {
      def regParamL1Fun = (index: Int) => {
        // Remove the L1 penalization on the intercept
        if (index == numFeatures) {
          0.0
        } else {
          if ($(standardization)) {
            regParamL1
          } else {
            // If `standardization` is false, we still standardize the data
            // to improve the rate of convergence; as a result, we have to
            // perform this reverse standardization by penalizing each component
            // differently to get effectively the same objective function when
            // the training dataset is not standardized.
            if (featuresStd(index) != 0.0) regParamL1 / featuresStd(index) else 0.0
          }
        }
      }
      new BreezeOWLQN[Int, BDV[Double]](addedIterations, 10, regParamL1Fun, $(tol))
    }

    val initialCoefficientsWithIntercept = Vectors.dense(
      if ($(fitIntercept)) oldModel.weights.toArray :+ oldModel.intercept
      else oldModel.weights.toArray)

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialCoefficientsWithIntercept.toBreeze.toDenseVector)

    val (coefficients, intercept, objectiveHistory) = {
      /*
      Note that in Logistic Regression, the objective history (loss + regularization)
      is log-likelihood which is invariance under feature standardization. As a result,
      the objective history from optimizer is the same as the one in the original space.
      */
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        state = states.next()
        arrayBuilder += state.adjustedValue
      }

      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        logError(msg)
        throw new SparkException(msg)
      }

      /*
      The coefficients are trained in the scaled space; we're converting them back to
      the original space.
      Note that the intercept in scaled space and original space is the same;
      as a result, no scaling is needed.
      */
      val rawCoefficients = state.x.toArray.clone()
      var i = 0
      while (i < numFeatures) {
        rawCoefficients(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
        i += 1
      }

      if ($(fitIntercept)) {
        (Vectors.dense(rawCoefficients.dropRight(1)).compressed, rawCoefficients.last,
          arrayBuilder.result())
      } else {
        (Vectors.dense(rawCoefficients).compressed, 0.0, arrayBuilder.result())
      }
    }

    if (handlePersistence) instances.unpersist()
      val model = copyValues(new LogisticRegressionModel(uid, coefficients, intercept))

    val logRegSummary = new BinaryLogisticRegressionTrainingSummary(
      model.transform(dataset),
      $(probabilityCol),
      $(labelCol),
      $(featuresCol),
      objectiveHistory)
    model.setSummary(logRegSummary)
  }

}

object WahooLogisticRegression extends DefaultParamsReadable[WahooLogisticRegression] {
  override def load(path: String): WahooLogisticRegression = super.load(path)
}


