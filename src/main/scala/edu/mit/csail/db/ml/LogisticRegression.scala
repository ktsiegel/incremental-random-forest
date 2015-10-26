package edu.mit.csail.db.ml

import java.io.OutputStream

import org.apache.spark.sql.DataFrame

class LogisticRegressionModel extends Model {
  override def predict(data: DataFrame): DataFrame = ???
  override def export(format: String, out: OutputStream) = ???
}

class LogisticRegressionEstimator extends Estimator[LogisticRegressionModel] {
  override def fit(data: DataFrame, modelSpec: ModelSpec): LogisticRegressionModel = ???
  override def addData(newData: DataFrame,
                       model: LogisticRegressionModel): LogisticRegressionModel = ???
}

class LogisticRegressionEvaluator extends Evaluator {
  override def evaluate(data: DataFrame): Double = ???
}
