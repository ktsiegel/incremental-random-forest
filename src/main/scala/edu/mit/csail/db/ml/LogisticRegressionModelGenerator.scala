package org.apache.spark.ml.classification

import org.apache.spark.mllib.linalg.Vector

class LogisticRegressionModelGenerator {
  def create(uid: String, coefficients: Vector, intercept: Double) =
    new LogisticRegressionModel(uid, coefficients, intercept)
}
