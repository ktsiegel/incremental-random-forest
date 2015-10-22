package edu.mit.csail.db.ml

import java.io.OutputStream

import org.apache.spark.sql.DataFrame

class LogisticRegression extends Estimator[LogisticRegressionModel] {
  /**
   * Fit a logistic regression model.
   * @param data: a DataFrame encapsulating the features that will be fit to the model
   * @param modelSpec: a ModelSpec object describing a model to be fit with the dataset
   * @return a model trained by the estimator
   */
  override def fit(data: DataFrame, modelSpec: ModelSpec): LogisticRegressionModel = ???

  /**
   * Update an existing linear regression model to create a new logistic regression model.
   * @param newData: The data frame to use in the training of the new model.
   * @param model: The model that will be retrained. It is not mutated.
   * @return The retrained model.
   */
  override def addData(newData: DataFrame,
                       model: LogisticRegressionModel): LogisticRegressionModel = ???
}
