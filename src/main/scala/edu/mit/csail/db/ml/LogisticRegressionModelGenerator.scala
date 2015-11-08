package org.apache.spark.ml.classification

import org.apache.spark.mllib.linalg.Vector

/**
 * Class for generating Spark-ML Logistic Regression Models.
 * Note that the package is org.apache.spark.ml.classification, because
 * the LogisticRegressionModel constructor is private to this package.
 * As a result, this separate generator class is necessary to be able
 * to generate LogisticRegressionModels from a set of parameters.
 */
class LogisticRegressionModelGenerator {
  /**
   * Creates a LogisticRegressionModel from the uid, coefficients, and
   * intercept of a previously existing LogisticRegressionModel.
   * @param uid - A unique ID string of a LogisticRegressionModel.
   * @param coefficients - The coefficients of the model.
   * @param intercept - The intercept of the model.
   * @return - Returns a LogisticRegressionModel with these parameters.
   */
  def create(uid: String, coefficients: Vector, intercept: Double) =
    new LogisticRegressionModel(uid, coefficients, intercept)
}
