package edu.mit.csail.db.ml

import java.io.OutputStream

import org.apache.spark.sql.DataFrame


class LogisticRegressionEstimator extends Estimator[LogisticRegressionModel] 
{
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

class LogisticRegressionEvaluator 
{
    /**
      * Compares the predicted output of a model with the actual output
      * @param predicted: the outcome predicted by a model
      * @param actual: the actual outcome
      * @returns a Result object encapsulating various accuracy metrics
      */
    def eval(predicted: DataFrame, actual: DataFrame): Result = 
    {
        var correct:Int = 0

        // TODO: get this to compile
        //predicted.zipWithIndex.foreach { case(entry, index) =>
        //    if (predicted(i) == actual(i)) 
        //    {
        //        correct += 1
        //    }
        //}
    
        return new Result( (correct * 1.0 / predicted.count).toInt );
    }
}


class LogisticRegressionModel extends Model 
{
    /**
      * Predict target variables using this model.
      * By default, the output data frame should contain an output column called "prediction".
      * This should be overridden in subclasses and configurable by the user.
      * @param data
      * @return
      */
    def predict(data: DataFrame): DataFrame = 
    {
        // TODO: predict based on model
        // y = (alpha * x + beta + error > 0) ? 1 : 0
	return null
    }

    /**
      * Export model to a different format
      * @param format
      * @param out
      */
    def export(format: String, out:OutputStream) = 
    {
        // TODO
    }
}
