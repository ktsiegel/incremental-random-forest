package org.apache.spark.wahoo.estimator

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Estimator => SparkEstimator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.wahoo.model.{Model, ModelSpec}

class LREstimator[M <: Model] extends Estimator {
  /**
   * Method that fits a single model to a dataset and returns the trained model.
   * This is an abstract method that must be implemented by subclasses.
   * @param data: a DataFrame encapsulating the features that will be fit to the model
   * @param modelSpec: a ModelSpec object describing a model to be fit with the dataset
   * @return: a model trained by the estimator
   */
  def fitModel(data: DataFrame, modelSpec: ModelSpec): M {
    // TODO find existing method in spark ML lib that does this
    // insert new model into ModelDB
  }

  /**
   * Method that updates a model with additional features and returns the trained model.
   * This is an abstract method that must be implemented by subclasses.
   * @param data: a DataFrame encapsulating the new features
   * @param model: a Model that must be re-trained on the new features
   * @return: the updated Model
   */
  def updateModel(data: DataFrame, model: M): M {
    // get model from ModelDB
    // update model
    // update model in ModelDB
  }
}
