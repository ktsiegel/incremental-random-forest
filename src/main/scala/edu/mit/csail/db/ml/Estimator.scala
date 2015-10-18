package edu.mit.csail.db.ml

import org.apache.spark.sql.DataFrame
/**
 * Abstract class for estimators that fit one more more models onto data and update models
 * in response to more training data
 * @tparam M
 * Created by mvartak on 10/11/15.
 */

// TODO: should this be a trait or an abstract class?
abstract class Estimator[M <: Model] {
  // TODO: where are we storing training information like the optimization technique?

  /**
   * Fits a model spec to a given data frame.
   * @param data: The data frame containing the features that will be used to create the model.
   * @param modelSpec: The specification for the model that should be created.
   * @return The fitted model.
   */
  def fit(data: DataFrame, modelSpec: ModelSpec): M

  /**
   * Retrains a model given new data.
   * @param newData: The data frame to use in the training of the new model.
   * @param model: The model that will be retrained. It is not mutated.
   * @return The retrained model.
   */
  def addData(newData: DataFrame, model: M): M

  /**
   * Fits a series of model specs to a given data frame and returns an array of models.
   * @param data: The data frame containing the features that will be used to create the models.
   * @param modelSpecs: The specifications for each model that should be created.
   * @return The fitted models.
   */
  def fit(data: DataFrame, modelSpecs: ModelSpec*): Seq[M] =
    for (spec <- modelSpecs) yield fit(data, spec)

  /**
   * Retrains the original array of models given new data.
   * @param newData: The data frame to used in the training of the new models.
   * @param models: The models that will be retrained. They are not mutated.
   * @return The retrained models.
   */
  def addData(newData: DataFrame, models: M*): Seq[M] =
    for (model <- models) yield addData(newData, model)
}
