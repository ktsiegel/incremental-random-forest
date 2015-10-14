package edu.mit.csail.db.ml

import org.apache.spark.ml.param.{ParamMap}
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
/**
 * Abstract class for estimators that fit one more more models onto data and update models
 * in response to more training data
 * @tparam M
 * Created by mvartak on 10/11/15.
 */

// TODO: should this be a trait or an abstract class?
abstract class Estimator[M <: Model[M]] {
  def fit(data: DataFrame, modelSpecs: Array[ModelSpec]): Array[M]
  // TODO: where are we storing training information like the optimization technique?

  def addData(newData: DataFrame, models: Array[M])
}
