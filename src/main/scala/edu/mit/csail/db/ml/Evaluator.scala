package edu.mit.csail.db.ml

import org.apache.spark.sql.DataFrame

abstract class Evaluator {
  def eval(predicted: DataFrame, actual: DataFrame) // TODO: need to figure out what the output will be
}