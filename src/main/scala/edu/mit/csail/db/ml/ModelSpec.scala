package edu.mit.csail.db.ml

import org.apache.spark.ml.Model

/**
 * A specification representing a model to be trained. Subclass this for different models and make
 * sure to implement equals() and hashCode().
 * @param features - The features to use in training.
 * @tparam M - The type of the model. For example, LogisticRegressionModel.
 */
class ModelSpec[M <: Model[M]](val features: Array[String]) {
  override def equals(o: Any): Boolean = o match {
    case that: ModelSpec[M] => features.deep == that.features.deep
    case _ => false
  }
  override def hashCode: Int = features.foldLeft[Int](0)(_ + _.hashCode())
}