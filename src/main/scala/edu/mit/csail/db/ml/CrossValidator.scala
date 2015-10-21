package edu.mit.csail.db.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import edu.mit.csail.db.ml.{Model, ModelSpec, Evaluator}

/**
 * This class is responsible for performing cross validation given a set of parameters
 * and models
 * Created by mvartak on 10/14/15.
 */
class CrossValidator (
  modelSpecs: Array[ModelSpec],
  evaluator: Evaluator,
  params: ParamMap,
  data: DataFrame) {

  var dataFolds: Array[DataFrame] = splitData(data, params.get("folds"))
  var estimator: Estimator = new LREstimator()
  var db: ModelDB = new ModelDB()
  
  // TODO do we want to use existing classes?
  // private val cv = new SparkCrossValidator()
  // def setEstimator(estimator: Estimator) =
  // cv.setEstimator(estimator.getSparkEstimator)
  // override def getSparkEstimator = cv.getEstimator
  // def setEvaluator(evaluator: Evaluator) = {
  //   // TODO(hsubrama): Add this as an evaluator of the cross validator.
  //   cv.setEvaluator(new BinaryClassificationEvaluator())
  // }

  private def splitData(df: DataFrame, param: Param): Array[DataFrame] =
    // create new dataframe array based on number of folds & instantiate dataframes
  // loop through features in dataframe and insert into dataframes
  // return dataframe array
  Array(new DataFrame())

  // currently will return a Result object holding the info we need
  // see Result class
  def run(): Array[Result] = {
    var results:Array[Result] = empty();
    modelSpecs foreach (modelSpec => {
      dataFolds foreach (dataFold => {
        // TODO(Katie) figure out more about datafolds
        var dataFoldA = randSelect(dataFold, 0.8);
        var dataFoldB = dataFold.subtract(dataFoldA); //TODO figure out function that does this
        var inputA = dataFoldA.get("feature");
        var expectedA = dataFoldA.get("label");
        var inputB = dataFoldB.get("feature");
        var expectedB = dataFoldB.get("label");
        var model:Model = estimator.fit(modelSpec, inputA, expectedA, params);
        var res = Array(evaluator.eval(model.predict(inputB), expectedB));
        results = concat(results, res);
        db.update(model);
      })
    })
    return results;
  }
}
