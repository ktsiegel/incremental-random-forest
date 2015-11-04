package edu.mit.csail.db.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame

//TODO: Add more fields to this specification.
/**
 * A specification representing a logistic regression to train. It should include anything that would
 * be necessary for training a logistic regression model.
 * @param features - The features to use in training.
 * @param regParam - The regularization parameter.
 */
class LogisticRegressionSpec(override val features: Array[String], val regParam: Double)
  extends ModelSpec[LogisticRegressionModel](features) {
  override def equals(o: Any): Boolean = o match {
    case that: LogisticRegressionSpec => super.equals(o) && that.regParam == regParam
    case _ => false
  }
  override def hashCode(): Int = super.hashCode() + regParam.hashCode()
}

/**
 * A smarter logistic regression which caches old models in the model database and looks them up before
 * trying to retrain. Make sure to to call the setDb method to give it a model database.
 */
class WahooLogisticRegression extends LogisticRegression
with HasModelDb with CanCache[LogisticRegressionModel] {
  override def modelSpec(dataset: DataFrame): LogisticRegressionSpec =
    new LogisticRegressionSpec(dataset.columns, super.getRegParam)
}