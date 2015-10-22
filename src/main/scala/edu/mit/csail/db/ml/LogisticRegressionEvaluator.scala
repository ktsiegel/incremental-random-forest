package org.apache.spark.wahoo.evaluator

import org.apache.spark.mllib.linalg.

class LogisticRegressionEvaluator {
  /**
   * Compares the predicted output of a model with the actual output
   * @param predicted: the outcome predicted by a model
   * @param actual: the actual outcome
   * @returns a Result object encapsulating various accuracy metrics
   */
  def eval(predicted: DataFrame, actual: DataFrame): Result {
    var correct:Int = 0
    predicted.zipWithIndex.foreach( case(entry, index) =>
      if (predicted(i) == actual(i)) {
        correct += 1
      }
    )
    return (correct * 1.0 / predicted.length);
  }
}
