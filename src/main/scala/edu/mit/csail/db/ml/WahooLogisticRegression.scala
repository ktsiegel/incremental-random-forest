package org.apache.spark.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.DataFrame
import com.mongodb.casbah.Imports._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}

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

  private val dim = dimension; //hack

  private var totalCnt: Long = 0L //numbe of records we have seen so far
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

  // for((x,i) <- xs.view.zipWithIndex) println("String #" + i + " is " + x)

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
