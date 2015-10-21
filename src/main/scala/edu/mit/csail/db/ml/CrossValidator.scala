package edu.mit.csail.db.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import edu.mit.csail.db.ml.{Model, ModelSpec, Evaluator}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator => SparkCrossValidator}

/**
 * This class is responsible for performing cross validation given a set of parameters
 * and models
 * Created by mvartak on 10/14/15.
 */
class CrossValidator extends Estimator (
  modelSpecs: Array[ModelSpec],
  evaluator: Evaluator,
  params: ParamMap,
  data: DataFrame) {
  
  private val cv = new SparkCrossValidator()

  def setEstimator(estimator: Estimator) =
    cv.setEstimator(estimator.getSparkEstimator)
  override def getSparkEstimator = cv.getEstimator

  def setEvaluator(evaluator: Evaluator) = {
    // TODO(hsubrama): Add this as an evaluator of the cross validator.
    cv.setEvaluator(new BinaryClassificationEvaluator())
  }

  override def fit(modelSpecs: Array[ModelSpec],
    data: DataFrame,
    params: ParamMap): Array[Model] = {
    // split the data based on number of folds
    // TODO: how to represent splits of the data

    // train models on each fold

    val cvModel = cv.fit(data)
    // evaluate on the respective folds

    // average and return

    //TODO(hsubrama): Build the model class and add the cvModel above into the model.
    val convertCvModelToModels: Array[Model] = Array()
    return convertCvModelToModels
  }
}

