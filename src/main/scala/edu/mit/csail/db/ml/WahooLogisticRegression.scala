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
with MultiThreadTrain[LogisticRegressionModel] {
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

  override def modelSpec(dataset: DataFrame): LogisticRegressionSpec =
    new LogisticRegressionSpec(dataset.columns, super.getRegParam, super.getMaxIter)


  // MV: pass in model specs to train
  protected def train(
                       dataset: DataFrame,
                       elasticNetParams: Array[Double],
                       regParams: Array[Double]): Array[LogisticRegressionModel] = {
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

    // MV: this can be multiple things
    val regParamL1 = Array[Double](elasticNetParams.length)
    val regParamL2 = Array[Double](elasticNetParams.length)

    for ((value, idx) <- regParamL1.view.zipWithIndex) {
      regParamL1(idx) = elasticNetParams(idx) * regParams(idx)
      regParamL2(idx) = (1 - elasticNetParams(idx)) * regParams(idx)
    }

    // MV: create multiple optimizers
    val optimizers = Array.ofDim[BreezeLBFGS[BDV[Double]]](elasticNetParams.length)
    for ((value, idx) <- regParamL1.view.zipWithIndex) {
      optimizers(idx) = if (elasticNetParams(idx) == 0.0 || regParams(idx) == 0.0) {
        new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol)) // MV: using the same maxIter and tol. Problem?
      } else {
        def regParamL1Fun = (index: Int) => {
          // Remove the L1 penalization on the intercept
          if (index == numFeatures) {
            0.0
          } else {
            if ($(standardization)) { // use across all models
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
        new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
      }
    }

    val initialWeightsWithIntercept =
      Vectors.zeros(if ($(fitIntercept)) numFeatures + 1 else numFeatures) //TODO: fit intercept assumed to be same

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

    val initWeights = optimizers.map(_ => initialWeightsWithIntercept)
    val fitIntercepts = optimizers.map (_ => $(fitIntercept))
    val standardizations = optimizers.map(_ => $(standardization))

    // MV: this is the thing shared by all the optimizers
    val costFunHelper = new MultiLogisticCostFunHelper(instances, numClasses, optimizers.length,
      fitIntercepts, standardizations, featuresStd, featuresMean, regParamL2, initWeights)

    val stateIterators = for (i <- optimizers.indices) yield optimizers(i).iterations(
      new CachedDiffFunction(new MultiLogisticCosFun(costFunHelper, i)),
      initialWeightsWithIntercept.toBreeze.toDenseVector)

    val histories = optimizers.map(_ => mutable.ArrayBuilder.make[Double])

    val currState = Array[Any](optimizers.length) // current state for each optimizer

    // TODO: optimize for when only updating one or the other
    val keepIterating = optimizers.map(_ => true)
    while (keepIterating.reduce((a, b) => a && b)) {
      // go through each optimizer and take a step
      for ((stateIterator, idx) <- stateIterators.view.zipWithIndex) {
        if (keepIterating(idx) && stateIterator.hasNext) {
          val state = stateIterator.next() // MV: THIS IS TAKING THE NEXT STEP
          currState(idx) =  state
          histories(idx) += state.adjustedValue
          if (state == null) {
            val msg = s"${optimizers(idx).getClass.getName} failed."
            logError(msg)
            throw new SparkException(msg)
          }
          costFunHelper.update(idx, state.x)
        } else {
          keepIterating(idx) = false
        }
      }
    }

    val models = Array.ofDim[LogisticRegressionModel](optimizers.length)

    for ((value, idx) <- optimizers.view.zipWithIndex) {
      val (weights, intercept, objectiveHistory) = {
        /*
           The weights are trained in the scaled space; we're converting them back to
           the original space.
           Note that the intercept in scaled space and original space is the same;
           as a result, no scaling is needed.
         */
        val opt = optimizers(idx)
        val rawWeights = currState(idx).asInstanceOf[opt.State].x.toArray.clone()

//        match {
//          case w : optimizers(idx).State.type => w.x.toArray.clone()
//          case _ => null
//        }



        var i = 0
        while (i < numFeatures) {
          rawWeights(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
          i += 1
        }

        if ($(fitIntercept)) {
          (Vectors.dense(rawWeights.dropRight(1)).compressed, rawWeights.last, histories(idx).result())
        } else {
          (Vectors.dense(rawWeights).compressed, 0.0, histories(idx).result())
        }
      }

      models(idx) = copyValues(new LogisticRegressionModel(uid, weights, intercept))

      val logRegSummary = new BinaryLogisticRegressionTrainingSummary(
        models(idx).transform(dataset),
        $(probabilityCol),
        $(labelCol),
        objectiveHistory)
      models(idx).setSummary(logRegSummary)
    }
    // after all models have been train
    if (handlePersistence) instances.unpersist()
    models
  }
}

/**
 * Multiple aggregator version of LogisticAggregator.
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

  private val dim = dimension; //hack. number of columns in the data

  private var totalCnt: Long = 0L //number of records we have seen so far
  private val lossSums: Array[Double] = Array[Double](numModels) // the values of loss for all models

  private val weightArrays = weights.map(weight => weight match { // array of weights for various models
      case dv: DenseVector => dv.values
      case _ =>
        throw new IllegalArgumentException(
          s"weights only supports dense vector but got type ${weight.getClass}.")
    })

  // MV: removed definition of dim, not dim is passed in

  private val gradientSumArrays = weightArrays.map(arr => Array.ofDim[Double](arr.length)) // arrays of gradients
  // w.r.t. each weight

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
        val sums = Array[Double](numModels)
        data.foreachActive { (index, value) =>
          // go through the weight vectors
          if (featuresStd(index) != 0.0 && value != 0.0) {
            for ((wValue, wIndex) <- localWeightArrays.view.zipWithIndex) {
              sums(wIndex) += wValue(index) * (value / featuresStd(index))
            }
          }
        }

        // TODO: IMP. check how intercept stored
        for ((sum, index) <- sums.view.zipWithIndex) {
          sums(index) += (if (fitIntercepts(index)) localWeightArrays(index)(dim) else 0.0)
          // this should be ok because dim columns, dim-th column intercept
        }

        val margins = sums.map(x => -x)

        val multipliers = margins.map(margin => (1.0 / (1.0 + math.exp(margin))) - label)

        data.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            for ((gValue, gIndex) <- localGradientSumArrays.view.zipWithIndex) {
              gValue(index) += multipliers(gIndex) * (value / featuresStd(index))
            }
          }
        }

        // TODO: again, how is intercept stored?
        for ((fitIntercept, index) <- fitIntercepts.view.zipWithIndex) {
          if (fitIntercept) localGradientSumArrays(index)(dim) += multipliers(index)
        }

        // TODO: make scala-esque
        for ((_, index) <- lossSums.view.zipWithIndex) {
          if (label > 0) {
            // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
            lossSums(index) += MLUtils.log1pExp(margins(index))
          } else {
            lossSums(index) += MLUtils.log1pExp(margins(index)) - margins(index)
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
    for ((_, idx) <- result.view.zipWithIndex) {
      result(idx) = Vectors.dense(gradientSumArrays(idx).clone())
      scal(1.0/totalCnt, result(idx))
    }
    result
  }
}

private class MultiLogisticCosFun(helper: MultiLogisticCostFunHelper, idx: Int) extends DiffFunction[BDV[Double]] {
  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    helper.calculate(weights, idx)
  }
}

/**
 * LogisticCostFun implements Breeze's DiffFunction[T] for a multinomial logistic loss function,
 * as used in multi-class classification (it is also used in binary logistic regression).
 * It returns the loss and gradient with L2 regularization at a particular point (weights).
 * It's used in Breeze's convex optimization routines.
 */
private class MultiLogisticCostFunHelper(
                               data: RDD[(Double, Vector)],
                               numClasses: Int,
                               numModels: Int,
                               fitIntercepts: Array[Boolean],
                               standardizations: Array[Boolean], // MV: ok?
                               featuresStd: Array[Double],
                               featuresMean: Array[Double],
                               regParamsL2: Array[Double],
                               var weightArrayCurr: Array[Vector]) { // this the initial values of weights
  var multiLogisticAggregator: MultiLogisticAggregator = null
  var weightArrayNext = weightArrayCurr.map(w => w.toBreeze.toDenseVector)

  def update(idx: Int, value: BDV[Double]): Unit = {
    weightArrayNext(idx) = value
  }

  def calculate(weights: BDV[Double], idx: Int): (Double, BDV[Double]) = {
    val numFeatures = featuresStd.length

    if (idx == 0) { // start of new iteration, need to recompute aggregate
      weightArrayCurr = weightArrayNext.map(w => Vectors.fromBreeze(w)) // turn to spark vector
      // compute new value
      multiLogisticAggregator = data.treeAggregate(new MultiLogisticAggregator(
        numModels, numFeatures, weightArrayCurr, numClasses, fitIntercepts, featuresStd, featuresMean))(
        seqOp = (c, v) => (c, v) match {
          case (aggregator, (label, features)) => aggregator.add(label, features)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
        })
    }

    val totalGradientArray = multiLogisticAggregator.gradient(idx).toArray // MV: this can be cached

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