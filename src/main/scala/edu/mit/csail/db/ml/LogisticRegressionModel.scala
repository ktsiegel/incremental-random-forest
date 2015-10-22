package edu.mit.csail.db.ml

import java.io.OutputStream
import org.apache.spark.sql.DataFrame

class LogisticRegressionModel extends Model {
  /**
   * Predict target variables using this model.
   * By default, the output data frame should contain an output column called "prediction".
   * This should be overridden in subclasses and configurable by the user.
   * @param data
   * @return
   */
  def predict(data: DataFrame): DataFrame {
    // TODO: predict based on model
    // y = (alpha * x + beta + error > 0) ? 1 : 0
  }

  /**
   * Export model to a different format
   * @param format
   * @param out
   */
  def export(format: String, out:OutputStream) {
    // TODO
  }
}
