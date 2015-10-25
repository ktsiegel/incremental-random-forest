package edu.mit.csail.db.ml

import org.apache.spark.sql.DataFrame

abstract class Evaluator {
  /**
   * Compares the predicted output of a model with the actual output.
   *
   * Note that subclasses should contain logic for specifying which columns contain the labels.
   * @param predicted: the outcome predicted by a model
   * @param actual: the actual outcome
   * @return a Result object encapsulating various accuracy metrics
   */
  def eval(predicted: DataFrame, actual: DataFrame): Result
}
