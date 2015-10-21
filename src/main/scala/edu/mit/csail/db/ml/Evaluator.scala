package edu.mit.csail.db.ml

import org.apache.spark.sql.DataFrame

abstract class Evaluator {
  /**
   * Compares the predicted output of a model with the actual output
   * @param predicted: the outcome predicted by a model
   * @param actual: the actual outcome
   * @returns a Result object encapsulating various accuracy metrics
   */
  def eval(predicted: DataFrame, actual: DataFrame): Result
  // TODO: need to figure out what the output will be
}
