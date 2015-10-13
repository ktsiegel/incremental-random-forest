package edu.mit.csail.db

import org.apache.spark.ml.param.{ParamMap}
import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
/**
 * Abstract class for estimators that fit one more more models onto data and update models
 * in response to more training data
 * @tparam M
 * Created by mvartak on 10/11/15.
 */
abstract class Estimator[M <: Model[M]] {
  def fit(dataset: DataFrame, modelSpecs: Array[ModelSpec], paramMap: ParamMap): Array[M]

  def addData(dataset: DataFrame, models: Array[M])
}
