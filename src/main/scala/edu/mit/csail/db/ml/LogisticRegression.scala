package edu.mit.csail.db.ml

import java.io.OutputStream

import org.apache.spark.sql.DataFrame



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
