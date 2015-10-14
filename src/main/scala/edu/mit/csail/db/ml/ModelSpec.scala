package edu.mit.csail.db.ml

import org.apache.spark.ml.param.{ParamMap}

/**
 * ModelSpec contains the specification for a model, i.e. the properties that define a model
 * including model type, features and parameters.
 * Created by mvartak on 10/11/15.
 */
class ModelSpec (
  val modelType: String, // TODO: Maybe this is something every model spec overrides?
  val featureSet: Array[String], // TODO: list of string for now; should this change?
  var params: ParamMap) { // this includes all parameters
}
