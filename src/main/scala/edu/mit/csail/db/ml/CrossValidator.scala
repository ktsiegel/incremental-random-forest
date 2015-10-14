package edu.mit.csail.db.ml

import edu.mit.csail.db.ml.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

/**
 * This class is responsible for performing cross validation given a set of parameters
 * and models
 * Created by mvartak on 10/14/15.
 */
class CrossValidator ( // TODO: should this extend estimator similar to spark.ml?
  modelSpecs: Array[ModelSpec],
  evaluator: Evaluator,
  params: ParamMap,
  data: DataFrame) {

  def cv() { // TODO: need to figure out what the output will be
    // split the data based on number of folds
    // TODO: how to represent splits of the data
    var Array[DataFrame]

    // train models on each fold

    // evaluate on the respective folds

    // average and return
  }
}
