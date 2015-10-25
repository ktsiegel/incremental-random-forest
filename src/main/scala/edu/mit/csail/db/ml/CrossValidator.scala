package edu.mit.csail.db.ml

// TODO - I had a look at this class in Spark and it’s private to the ml package. We’ll have to hack in something else.
//import org.apache.spark.ml.param.ParamMap

import org.apache.spark.sql.DataFrame

/**
 * This class is responsible for performing cross validation given a set of parameters
 * and models
 * Created by mvartak on 10/14/15.
 */
class CrossValidator (
  modelSpecs: Array[ModelSpec],
  evaluator: Evaluator,
  params: Array[Int],
  data: DataFrame,
  estimator: Estimator[Model]) {

    var dataFolds: Array[DataFrame] = splitData(data, params(0))
    var db: ModelDB = new ModelDB()
  
    // TODO do we want to use existing classes?
    // private val cv = new SparkCrossValidator()
    // cv.setEstimator(estimator.getSparkEstimator)
    // cv.setEvaluator(new BinaryClassificationEvaluator())

    // create new dataframe array based on number of folds & instantiate dataframes
    // loop through features in dataframe and insert into dataframes
    // return dataframe array
    private def splitData(df: DataFrame, param: Int): Array[DataFrame] =
    {
        return Array[DataFrame]()
    }

    // currently will return a Result object holding the info we need
    // see Result class
    def run(): Array[Result] = 
    {
      var results:Array[Result] = Array[Result]()
      modelSpecs foreach (modelSpec => {
        dataFolds foreach (dataFold => {
        // TODO(Katie) figure out more about datafolds
	// NOTE - The stuff commented out below does not compile.
        //var dataFoldA = randSelect(dataFold, 0.8)
        //var dataFoldB = dataFold.subtract(dataFoldA) //TODO figure out function that does this

        //var inputA = dataFoldA.get("feature")
        //var expectedA = dataFoldA.get("label")
        //var inputB = dataFoldB.get("feature")
        //var expectedB = dataFoldB.get("label")
        
	//var model: Model = estimator.fit(modelSpec, inputA, expectedA, params)
        //var res = Array(evaluator.eval(model.predict(inputB), expectedB))
        
	//results :+ res
        
	var query = " "
	// TODO: construct update query with 'model' variable 
        db.update(query)
      })
    })
    return results;
  }
}
