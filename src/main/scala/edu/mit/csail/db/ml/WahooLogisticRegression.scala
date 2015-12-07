package org.apache.spark.ml.classification

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import com.mongodb.casbah.Imports._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkException}
import org.apache.spark.ml.{HasModelDb, CanCache, MultiThreadTrain, WahooContext, ModelSpec, DBObjectHelper}
import breeze.optimize.FirstOrderMinimizer
import scala.collection.mutable
import java.util.concurrent.{CyclicBarrier, Phaser}

//TODO: Add more fields to this specification.
/**
 * A specification representing a logistic regression to train. It should include anything that would
 * be necessary for training a logistic regression model.
 * @param features - The features to use in training.
 * @param regParam - The regularization parameter.
 */
class LogisticRegressionSpec(override val features: Array[String], val regParam: Double, val maxIter: Int)
  extends ModelSpec[LogisticRegressionModel](features) {

  override def equals(o: Any): Boolean = o match {
    case that: LogisticRegressionSpec => super.equals(o) && that.regParam == regParam
    case _ => false
  }

  override def hashCode(): Int = super.hashCode() + regParam.hashCode()

  override def toDBObject(model: LogisticRegressionModel): MongoDBObject =
    DBObject(
      "uid" -> model.uid,
      "weights" -> model.weights.toArray,
      "intercept" -> model.intercept,
      "modelspec" -> DBObject(
        "type" -> "LogisticRegressionModel",
        "features" -> features,
        "regParam" -> regParam,
        "maxIter" -> maxIter
      )
    )

  override def toDBQuery(): MongoDBObject = 
    DBObject("modelspec" -> DBObject(
      "type" -> "LogisticRegressionModel",
      "features" -> features,
      "regParam" -> regParam,
      "maxIter" -> maxIter
    ))

  override def generateModel(dbObject: DBObjectHelper): LogisticRegressionModel = {
    new LogisticRegressionModel(dbObject.asString("uid"),
      Vectors.dense(dbObject.asList[Double]("weights").toArray),
      dbObject.asDouble("intercept")
    )
  }

  override def toString(): String = {
    val featureString = features.mkString("[", ", ", "]")
    s"LogisticRegression(features=$featureString, regParam=$regParam, maxIter=$maxIter)"
  }
}

/**
 * A smarter logistic regression which caches old models in the model database and looks them up before
 * trying to retrain. Make sure to to call the setDb method to give it a model database.
 */
class WahooLogisticRegression(uid: String, wc: WahooContext) extends LogisticRegression(uid)
with HasModelDb with CanCache[LogisticRegressionModel]
{
  this.setDb(wc.modelDB) // TODO: may go away if we change the cancache traits
  var trainingSummary: Option[LogisticRegressionTrainingSummary] = None

  override def train(dataset: DataFrame): LogisticRegressionModel = {
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

  def this(wc: WahooContext) = this(Identifiable.randomUID("logreg"), wc)

  override def copy(extra: ParamMap): WahooLogisticRegression = {
    val wlr = new WahooLogisticRegression(wc)
    copyValues(wlr, extra)
  }

  override def modelSpec(dataset: DataFrame): LogisticRegressionSpec =
    new LogisticRegressionSpec(dataset.columns, super.getRegParam, super.getMaxIter)

  // TODO: should this be a trait?
  def fitMulti(
      dataset: DataFrame,
      paramMapArray: Array[ParamMap]): Array[LogisticRegressionModel] = {

    val numModels = paramMapArray.length
    // extract params from the paramMapArray
    val elasticNetParams = Array.ofDim[Double](numModels)
    val regParams = Array.ofDim[Double](numModels)
    val maxIters = Array.ofDim[Int](numModels)
    val tols = Array.ofDim[Double](numModels)
    val standardizations = Array.ofDim[Boolean](numModels)
    val fitIntercepts = Array.ofDim[Boolean](numModels)

    // TODO: this is so crummy...
    for (i <- paramMapArray.indices) {
      val pm = this.extractParamMap(paramMapArray(i))
      elasticNetParams(i) = pm.get(this.elasticNetParam).get
      regParams(i) = pm.get(this.regParam).get
      maxIters(i) = pm.get(this.maxIter).get
      tols(i) = pm.get(this.tol).get
      standardizations(i) = pm.get(this.standardization).get
      fitIntercepts(i) = pm.get(this.fitIntercept).get
    }

    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val instances = extractLabeledPoints(dataset).map {
      case LabeledPoint(label: Double, features: Vector) => (label, features)
    }

    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val (summarizer, labelSummarizer) = instances.treeAggregate(
      (new MultivariateOnlineSummarizer, new MultiClassSummarizer))(
      seqOp = (c, v) => (c, v) match {
        case ((summarizer: MultivariateOnlineSummarizer, labelSummarizer: MultiClassSummarizer),
        (label: Double, features: Vector)) =>
          (summarizer.add(features), labelSummarizer.add(label))
      },
      combOp = (c1, c2) => (c1, c2) match {
        case ((summarizer1: MultivariateOnlineSummarizer,
        classSummarizer1: MultiClassSummarizer), (summarizer2: MultivariateOnlineSummarizer,
        classSummarizer2: MultiClassSummarizer)) =>
          (summarizer1.merge(summarizer2), classSummarizer1.merge(classSummarizer2))
      })

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

    print("FeaturesMean: " )
    featuresMean.map(m => print(m + ","))

    val regParamL1 = Array.ofDim[Double](numModels)
    val regParamL2 = Array.ofDim[Double](numModels)

    for (idx <- 0 until numModels) {
      regParamL1(idx) = elasticNetParams(idx) * regParams(idx)
      regParamL2(idx) = (1 - elasticNetParams(idx)) * regParams(idx)
    }

    // ok to claim type as LBFGS, OWLQN is a subclass
    val optimizers = Array.ofDim[BreezeLBFGS[BDV[Double]]](numModels)
    for (idx <- 0 until numModels) {
      optimizers(idx) = if (elasticNetParams(idx) == 0.0 || regParams(idx) == 0.0) { // l2 or none
        new BreezeLBFGS[BDV[Double]](maxIters(idx), 10, tols(idx))
      } else {
        def regParamL1Fun = (index: Int) => {
          // Remove the L1 penalization on the intercept
          if (index == numFeatures) {
            0.0
          } else {
            if (standardizations(idx)) {
              regParamL1(idx)
            } else {
              // If `standardization` is false, we still standardize the data
              // to improve the rate of convergence; as a result, we have to
              // perform this reverse standardization by penalizing each component
              // differently to get effectively the same objective function when
              // the training dataset is not standardized.
              if (featuresStd(index) != 0.0) regParamL1(idx) / featuresStd(index) else 0.0
            }
          }
        }
        new BreezeOWLQN[Int, BDV[Double]](maxIters(idx), 10, regParamL1Fun, tols(idx))
      }
    }

    val initWeights = for (i <- 0 until numModels) yield {
      if (fitIntercepts(i)) {
        val tmp = Vectors.zeros(numFeatures + 1)
        // init intercept based on relative frequency of 0/1
        tmp.toArray(numFeatures) = math.log(histogram(1).toDouble / histogram(0).toDouble)
        tmp
      } else {
        Vectors.zeros(numFeatures)
      }
    }

    //val barrier = new CyclicBarrier(numModels)
    val phaser = new Phaser(numModels)

    // the helper will be shared by all cost functions
    val costFunHelper = new MultiLogisticCostFunHelper(numModels, instances, numClasses,
      fitIntercepts, standardizations, featuresStd, featuresMean, regParamL2, phaser)
    val stateIterators = Array.ofDim[Any](numModels)

    val histories = optimizers.map(_ => mutable.ArrayBuilder.make[Double])

    val currState = Array.ofDim[Any](optimizers.length) // current state for each optimizer

    val threads = Array.ofDim[Thread](numModels)

    val done = Array.ofDim[Boolean](numModels)

    for (idx <- 0 until numModels) {
      threads(idx) = new Thread {
        override def run: Unit = {
          val optimizer = optimizers(idx)
          stateIterators(idx) = optimizer.iterations(
            new CachedDiffFunction(new MultiLogisticCosFun(costFunHelper, idx)),
            initWeights(idx).toBreeze.toDenseVector)
          val stateIterator = stateIterators(idx).asInstanceOf[Iterator[optimizer.State]]
          while (stateIterator.hasNext) {
            val state = stateIterator.next()
            currState(idx) = state
            histories(idx) += state.adjustedValue
            if (state == null) {
              val msg = s"${optimizers(idx).getClass.getName} failed."
              logError(msg)
              throw new SparkException(msg)
            }}
          phaser.arriveAndDeregister()

        }}}

    threads.map(thread => thread.start())

    threads.map(thread => thread.join())

    val models = Array.ofDim[LogisticRegressionModel](numModels)

    for (idx <- 0 until numModels) {
      val (weights, intercept, objectiveHistory) = {
        /*
           The weights are trained in the scaled space; we're converting them back to
           the original space.
           Note that the intercept in scaled space and original space is the same;
           as a result, no scaling is needed.
         */
        val opt = optimizers(idx)
        val rawWeights = currState(idx).asInstanceOf[opt.State].x.toArray.clone()

        for (i <- 0 until numFeatures) {
          rawWeights(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
        }

        if (fitIntercepts(idx)) {
          (Vectors.dense(rawWeights.dropRight(1)).compressed, rawWeights.last, histories(idx).result())
        } else {
          (Vectors.dense(rawWeights).compressed, 0.0, histories(idx).result())
        }
      }

      models(idx) = copyValues(new LogisticRegressionModel(uid, weights, intercept))

      val logRegSummary = new BinaryLogisticRegressionTrainingSummary(
        models(idx).transform(dataset),
        $(probabilityCol), // we use these as is
        $(labelCol), // same as above
        objectiveHistory)
      models(idx).setSummary(logRegSummary)
    }
    // after all models have been train
    if (handlePersistence) instances.unpersist()
    models
  }

  def fitCopy(dataset: DataFrame): LogisticRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val instances = extractLabeledPoints(dataset).map {
      case LabeledPoint(label: Double, features: Vector) => (label, features)
    }
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val (summarizer, labelSummarizer) = instances.treeAggregate(
      (new MultivariateOnlineSummarizer, new MultiClassSummarizer))(
      seqOp = (c, v) => (c, v) match {
        case ((summarizer: MultivariateOnlineSummarizer, labelSummarizer: MultiClassSummarizer),
        (label: Double, features: Vector)) =>
          (summarizer.add(features), labelSummarizer.add(label))
      },
      combOp = (c1, c2) => (c1, c2) match {
        case ((summarizer1: MultivariateOnlineSummarizer,
        classSummarizer1: MultiClassSummarizer), (summarizer2: MultivariateOnlineSummarizer,
        classSummarizer2: MultiClassSummarizer)) =>
          (summarizer1.merge(summarizer2), classSummarizer1.merge(classSummarizer2))
      })

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

    val costFun = new LogisticCostFun2(instances, numClasses, $(fitIntercept), $(standardization),
      featuresStd, featuresMean, regParamL2)

    val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
      new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
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
      new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
    }

    val initialWeightsWithIntercept =
      Vectors.zeros(if ($(fitIntercept)) numFeatures + 1 else numFeatures)

    if ($(fitIntercept)) {
      /*
         For binary logistic regression, when we initialize the weights as zeros,
         it will converge faster if we initialize the intercept such that
         it follows the distribution of the labels.

         {{{
         P(0) = 1 / (1 + \exp(b)), and
         P(1) = \exp(b) / (1 + \exp(b))
         }}}, hence
         {{{
         b = \log{P(1) / P(0)} = \log{count_1 / count_0}
         }}}
       */
      initialWeightsWithIntercept.toArray(numFeatures)
        = math.log(histogram(1).toDouble / histogram(0).toDouble)
    }

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialWeightsWithIntercept.toBreeze.toDenseVector)

    val (weights, intercept, objectiveHistory) = {
      /*
         Note that in Logistic Regression, the objective history (loss + regularization)
         is log-likelihood which is invariance under feature standardization. As a result,
         the objective history from optimizer is the same as the one in the original space.
       */
      var iters = 0
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        iters += 1
        state = states.next()
        arrayBuilder += state.adjustedValue
      }

      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        logError(msg)
        throw new SparkException(msg)
      }

      /*
         The weights are trained in the scaled space; we're converting them back to
         the original space.
         Note that the intercept in scaled space and original space is the same;
         as a result, no scaling is needed.
       */
      val rawWeights = state.x.toArray.clone()
      var i = 0
      while (i < numFeatures) {
        rawWeights(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
        i += 1
      }

      if ($(fitIntercept)) {
        (Vectors.dense(rawWeights.dropRight(1)).compressed, rawWeights.last, arrayBuilder.result())
      } else {
        (Vectors.dense(rawWeights).compressed, 0.0, arrayBuilder.result())
      }
    }

    if (handlePersistence) instances.unpersist()

    val model = copyValues(new LogisticRegressionModel(uid, weights, intercept))
    val logRegSummary = new BinaryLogisticRegressionTrainingSummary(
      model.transform(dataset),
      $(probabilityCol),
      $(labelCol),
      objectiveHistory)
    model.setSummary(logRegSummary)
  }
}

/**
 * Multiple aggregator version of LogisticAggregator.
 *
 * Computes the gradient and loss for binary logistic loss function, as used
 * in binary classification for samples in sparse or dense vector in a online fashion.
 *
 * Note that multinomial logistic loss is not supported yet!
 *
 * Two LogisticAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * @param numModels Number of models that are being trained
 * @param dim Number of fields in the dataset (irrespective of whether intercept is used)
 * @param weights The weights/coefficients corresponding to the features.
 * @param numClasses the number of possible outcomes for k classes classification problem in
 *                   Multinomial Logistic Regression.
 * @param fitIntercept Whether to fit an intercept term.
 * @param featuresStd The standard deviation values of the features.
 * @param featuresMean The mean values of the features.
 */
private class MultiLogisticAggregator(
    numModels: Int,
    dimension: Int,
    weights: Array[Vector],
    numClasses: Int,
    fitIntercepts: Array[Boolean],
    featuresStd: Array[Double], // only one because all features present in df
    featuresMean: Array[Double]) extends Serializable {

  private val dim = dimension; // number of features

  private var totalCnt: Long = 0L //number of records we have seen so far
  private val lossSums: Array[Double] = Array.ofDim[Double](numModels) // the values of loss for all models

  private val weightArrays = weights.map(weight => weight match { // array of weights for various models
      case dv: DenseVector => dv.values
      case _ =>
        throw new IllegalArgumentException(
          s"weights only supports dense vector but got type ${weight.getClass}.")
    })

  // arrays of gradients w.r.t. each weight
  private val gradientSumArrays = weightArrays.map(arr => Array.ofDim[Double](arr.length))

  /**
   * Add a new training data to this LogisticAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param label The label for this data point.
   * @param data The features for one data point in dense/sparse vector format to be added
   *             into this aggregator.
   * @return This LogisticAggregator object.
   */
  def add(label: Double, data: Vector): this.type = {
    require(dim == data.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $dim but got ${data.size}.")

    val localWeightArrays = weightArrays
    val localGradientSumArrays = gradientSumArrays

    numClasses match {
      case 2 =>
        // For Binary Logistic Regression.
        // TODO: probably not scala-esque yet. when rewriting note that we want to process each data
        // tuple only once and update all models at the same time

        val sums = Array.ofDim[Double](numModels) // store  W^T x for all models

        data.foreachActive { (index, value) =>
          // go through the weight vectors, compute w_i * x_i (not including intercept)
          if (featuresStd(index) != 0.0 && value != 0.0) {
            for (modelIdx <- 0 until numModels) {
              sums(modelIdx) += localWeightArrays(modelIdx)(index) * (value / featuresStd(index))
            }
          }
        }

        // add intercept value
        for (modelIdx <- 0 until numModels) {
          sums(modelIdx) += (if (fitIntercepts(modelIdx)) localWeightArrays(modelIdx)(dim) else 0.0)
          // this should be ok because dim columns, dim-th column intercept
        }
        // W^T x computed

        val margins = sums.map(x => -x)

        // compute (1/(1 + e^(-W^Tx))) - y
        val multipliers = margins.map(margin => (1.0 / (1.0 + math.exp(margin))) - label)

        data.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            for (modelIdx <- 0 until numModels) {
              localGradientSumArrays(modelIdx)(index) += multipliers(modelIdx) * (value / featuresStd(index))
            }}}

        for (modelIdx <- 0 until numModels) {
          if (fitIntercepts(modelIdx)) {
            localGradientSumArrays(modelIdx)(dim) += multipliers(modelIdx)
          }
        }

        for (modelIdx <- 0 until numModels) {
          if (label > 0) {
            // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
            lossSums(modelIdx) += MLUtils.log1pExp(margins(modelIdx))
          } else {
            lossSums(modelIdx) += MLUtils.log1pExp(margins(modelIdx)) - margins(modelIdx)
            // TODO: check that i'm computing margin correctly
          }
        }
      case _ =>
        new NotImplementedError("LogisticRegression with ElasticNet in ML package only supports " +
          "binary classification for now.")
    }
    totalCnt += 1
    this
  }

  /**
   * Merge another LogisticAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other LogisticAggregator to be merged.
   * @return This LogisticAggregator object.
   */
  def merge(other: MultiLogisticAggregator): this.type = {
    require(this.dim == other.dim, s"Dimensions mismatch when merging with another " +
      s"LeastSquaresAggregator. Expecting ${this.dim} but got ${other.dim}.")

    if (other.totalCnt != 0) {
      totalCnt += other.totalCnt
      addTo(lossSums, other.lossSums)

      var i = 0
      val localThisgradientSumArrays = this.gradientSumArrays
      val localOthergradientSumArrays = other.gradientSumArrays
      val len = localThisgradientSumArrays.length
      while (i < len) {
        addTo(localThisgradientSumArrays(i), localOthergradientSumArrays(i))
        i += 1
      }
    }
    this
  }

  def addTo(x: Array[Double], y: Array[Double]): Unit = {
    for ((_, idx) <- x.view.zipWithIndex) {
      x(idx) += y(idx)
    }
  }

  def count: Long = totalCnt

  def loss: Array[Double] = {
    lossSums.map(ls => ls/totalCnt)
  }

  def gradient: Array[Vector] = {
    val result = Array.ofDim[Vector](numModels)
    for (idx <- 0 until numModels) {
      result(idx) = Vectors.dense(gradientSumArrays(idx).clone())
      scal(1.0/totalCnt, result(idx))
    }
    result
  }
}

/**
 * This class calculates the cost function for logistic regression.
 * It implements optimizations for when multiple models are being trained,
 * hence the name
 * @param helper Helper class that does the heavy lifting
 * @param idx Index of the model being trained
 */
private class MultiLogisticCosFun(helper: MultiLogisticCostFunHelper, idx: Int)
  extends DiffFunction[BDV[Double]] {
  var iters = 0
  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    iters += 1
    helper.calculate(weights, idx, iters)
  }
}

/**
 * MultiLogisticCostFunHelper is a cost function helper shared by all cost functions used in
 * multi-model training
 *
 * The calculate function is called repeatedly by the breeze optimizer to get the loss
 * and the gradientArray
 *
 * Works as follows, for each iteration:
 * - The first time the calculate function is called, it uses the multiLogisticAggregator to
 * compute gradients and losses, and caches them (internally in aggregator)
 * - on a subsequent call, the pre-computed gradients and losses are just reused
 * - in either case, the new weights are stored in weightArrayNext
 */

// TODO: why is L1 not supported?
private class MultiLogisticCostFunHelper(
     numModels: Int,
     data: RDD[(Double, Vector)],
     numClasses: Int,
     fitIntercepts: Array[Boolean],
     standardizations: Array[Boolean],
     featuresStd: Array[Double],
     featuresMean: Array[Double],
     regParamsL2: Array[Double],
     phaser: Phaser) {
     // barrier: CyclicBarrier) {

  var multiLogisticAggregator: MultiLogisticAggregator = null // aggregator used to compute loss
  var weightArrayNext = Array.ofDim[BDV[Double]](numModels)
  var weightArrayCurr = Array.ofDim[Vector](numModels)
  var helperIter = 0
  var done = Array.ofDim[Boolean](numModels)
  var multiGradientArray: Array[Vector] = null

  // called by the breeze optimizer to get the loss and gradient array
  def calculate(weights: BDV[Double], idx: Int /* model Idx */, iter: Int): (Double, BDV[Double]) = {
    weightArrayNext(idx) = weights
    phaser.arriveAndAwaitAdvance() // wait until all threads have updated the weights

    this.synchronized {
      if (helperIter < iter) {
        weightArrayCurr = weightArrayNext.map(w => Vectors.fromBreeze(w))
        val numFeatures = featuresStd.length
        multiLogisticAggregator = data.treeAggregate(new MultiLogisticAggregator(numModels,
          numFeatures, weightArrayCurr, numClasses, fitIntercepts, featuresStd, featuresMean))(
          seqOp = (c, v) => (c, v) match {
            case (aggregator, (label, features)) => aggregator.add(label, features)
          },
          combOp = (c1, c2) => (c1, c2) match {
            case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
          })
        helperIter += 1
        multiGradientArray = multiLogisticAggregator.gradient
      }
    }

    val totalGradientArray = multiGradientArray(idx).toArray
    val numFeatures = featuresStd.length
    // regVal is the sum of weight squares excluding intercept for L2 regularization.
    val w = weightArrayCurr(idx)
    val regVal = if (regParamsL2(idx) == 0.0) {
      0.0
    } else {
      var sum = 0.0
      w.foreachActive { (index, value) =>
        // If `fitIntercept` is true, the last term which is intercept doesn't
        // contribute to the regularization.
        if (index != numFeatures) {
          // The following code will compute the loss of the regularization; also
          // the gradient of the regularization, and add back to totalGradientArray.
          sum += {
            if (standardizations(idx)) {
              totalGradientArray(index) += regParamsL2(idx) * value
              value * value
            } else {
              if (featuresStd(index) != 0.0) {
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                val temp = value / (featuresStd(index) * featuresStd(index))
                totalGradientArray(index) += regParamsL2(idx) * temp
                value * temp
              } else {
                0.0
              }
            }
          }
        }
      }
      0.5 * regParamsL2(idx) * sum
    }
    (multiLogisticAggregator.loss(idx) + regVal, new BDV(totalGradientArray))
  }
}

private class LogisticCostFun2(
                               data: RDD[(Double, Vector)],
                               numClasses: Int,
                               fitIntercept: Boolean,
                               standardization: Boolean,
                               featuresStd: Array[Double],
                               featuresMean: Array[Double],
                               regParamL2: Double) extends DiffFunction[BDV[Double]] {
  var iters = 0
  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    iters += 1
    val numFeatures = featuresStd.length
    val w = Vectors.fromBreeze(weights)

    val logisticAggregator = data.treeAggregate(new LogisticAggregator2(w, numClasses, fitIntercept,
      featuresStd, featuresMean))(
      seqOp = (c, v) => (c, v) match {
        case (aggregator, (label, features)) => aggregator.add(label, features)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
      })

    val totalGradientArray = logisticAggregator.gradient.toArray

    // regVal is the sum of weight squares excluding intercept for L2 regularization.
    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      w.foreachActive { (index, value) =>
        // If `fitIntercept` is true, the last term which is intercept doesn't
        // contribute to the regularization.
        if (index != numFeatures) {
          // The following code will compute the loss of the regularization; also
          // the gradient of the regularization, and add back to totalGradientArray.
          sum += {
            if (standardization) {
              totalGradientArray(index) += regParamL2 * value
              value * value
            } else {
              if (featuresStd(index) != 0.0) {
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                val temp = value / (featuresStd(index) * featuresStd(index))
                totalGradientArray(index) += regParamL2 * temp
                value * temp
              } else {
                0.0
              }
            }
          }
        }
      }
      0.5 * regParamL2 * sum
    }

    (logisticAggregator.loss + regVal, new BDV(totalGradientArray))
  }
}

private class LogisticAggregator2(
                                  weights: Vector,
                                  numClasses: Int,
                                  fitIntercept: Boolean,
                                  featuresStd: Array[Double],
                                  featuresMean: Array[Double]) extends Serializable {

  private var totalCnt: Long = 0L
  private var lossSum = 0.0

  private val weightsArray = weights match {
    case dv: DenseVector => dv.values
    case _ =>
      throw new IllegalArgumentException(
        s"weights only supports dense vector but got type ${weights.getClass}.")
  }

  private val dim = if (fitIntercept) weightsArray.length - 1 else weightsArray.length

  private val gradientSumArray = Array.ofDim[Double](weightsArray.length)

  /**
   * Add a new training data to this LogisticAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param label The label for this data point.
   * @param data The features for one data point in dense/sparse vector format to be added
   *             into this aggregator.
   * @return This LogisticAggregator object.
   */
  def add(label: Double, data: Vector): this.type = {
    require(dim == data.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $dim but got ${data.size}.")

    val localWeightsArray = weightsArray
    val localGradientSumArray = gradientSumArray

    numClasses match {
      case 2 =>
        // For Binary Logistic Regression.
        val margin = - {
          var sum = 0.0
          data.foreachActive { (index, value) =>
            if (featuresStd(index) != 0.0 && value != 0.0) {
              sum += localWeightsArray(index) * (value / featuresStd(index))
            }
          }
          sum + { if (fitIntercept) localWeightsArray(dim) else 0.0 }
        }

        val multiplier = (1.0 / (1.0 + math.exp(margin))) - label

        data.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            localGradientSumArray(index) += multiplier * (value / featuresStd(index))
          }
        }

        if (fitIntercept) {
          localGradientSumArray(dim) += multiplier
        }

        if (label > 0) {
          // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
          lossSum += MLUtils.log1pExp(margin)
        } else {
          lossSum += MLUtils.log1pExp(margin) - margin
        }
      case _ =>
        new NotImplementedError("LogisticRegression with ElasticNet in ML package only supports " +
          "binary classification for now.")
    }
    totalCnt += 1
    this
  }

  /**
   * Merge another LogisticAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other LogisticAggregator to be merged.
   * @return This LogisticAggregator object.
   */
  def merge(other: LogisticAggregator2): this.type = {
    require(dim == other.dim, s"Dimensions mismatch when merging with another " +
      s"LeastSquaresAggregator. Expecting $dim but got ${other.dim}.")

    if (other.totalCnt != 0) {
      totalCnt += other.totalCnt
      lossSum += other.lossSum

      var i = 0
      val localThisGradientSumArray = this.gradientSumArray
      val localOtherGradientSumArray = other.gradientSumArray
      val len = localThisGradientSumArray.length
      while (i < len) {
        localThisGradientSumArray(i) += localOtherGradientSumArray(i)
        i += 1
      }
    }
    this
  }

  def count: Long = totalCnt

  def loss: Double = lossSum / totalCnt

  def gradient: Vector = {
    val result = Vectors.dense(gradientSumArray.clone())
    scal(1.0 / totalCnt, result)
    result
  }
}


// TODO: [MV] [IMP] double check if the formulas are correct