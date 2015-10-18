package edu.mit.csail.db.ml

import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar.mock

class EstimatorSuite extends FunSuite {
  val myModel = mock[Model]
  val mySpec = mock[ModelSpec]
  val myDataFrame = mock[DataFrame]
  
  class MyEstimator extends Estimator[Model] {
    def fit(data: DataFrame, modelSpec: ModelSpec): Model = myModel
    def addData(newData: DataFrame, model: Model): Model = myModel
  }

  test("fit can fit multiple model specs") {
    val actual = new MyEstimator().fit(myDataFrame, mySpec, mySpec).toArray
    assert(actual.length == 2)
    assert(actual(0) == myModel)
    assert(actual(1) == myModel)
  }

  test("addData can retrain multiple models") {
    val actual = new MyEstimator().addData(myDataFrame, myModel, myModel).toArray
    assert(actual.length == 2)
    assert(actual(0) == myModel)
    assert(actual(1) == myModel)
  }
}
