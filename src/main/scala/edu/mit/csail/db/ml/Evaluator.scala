package edu.mit.csail.db.ml

import org.apache.spark.sql.DataFrame

abstract class Evaluator {
  /**
   * Compares predicted values with actual values.
   *
   * Subclasses should contain logic for specifying the input and output columns.
   *
   * @param data The data set containing the actual and predicted labels.
   * @return A numerical value indicating how well the values match up.
   */
  def evaluate(data: DataFrame): Double
}
