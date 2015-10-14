package edu.mit.csail.db.ml

import org.apache.spark.mllib.linalg.Vector

abstract class Evaluator {
  // TODO: should this be a vector or a data frame?
  def eval(predicted: Vector, actual: Vector) // TODO: need to figure out what the output will be
}