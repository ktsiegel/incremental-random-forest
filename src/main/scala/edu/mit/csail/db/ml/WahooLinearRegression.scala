package org.apache.spark.ml

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrame
import com.mongodb.casbah.Imports._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

//TODO: Add more fields to this specification.
/**
 * A specification representing a linear regression to train. It should include anything that would
 * be necessary for training a linear regression model.
 * @param features - The features to use in training.
 * @param regParam - The regularization parameter.
 */
class LinearRegressionSpec(override val features: Array[String], val regParam: Double, val maxIter: Int)
  extends ModelSpec[LinearRegressionModel](features) {

  override def equals(o: Any): Boolean = o match {
    case that: LinearRegressionSpec => super.equals(o) && that.regParam == regParam
    case _ => false
  }

  override def hashCode(): Int = super.hashCode() + regParam.hashCode()

  override def toDBObject(model: LinearRegressionModel): MongoDBObject =
    DBObject(
      "uid" -> model.uid,
      "weights" -> model.weights.toArray,
      "intercept" -> model.intercept,
      "modelspec" -> DBObject(
        "type" -> "LinearRegressionModel",
        "features" -> features,
        "regParam" -> regParam,
        "maxIter" -> maxIter
      )
    )

  override def toDBQuery(): MongoDBObject = 
    DBObject("modelspec" -> DBObject(
      "type" -> "LinearRegressionModel",
      "features" -> features,
      "regParam" -> regParam,
      "maxIter" -> maxIter
    ))

  override def generateModel(dbObject: DBObjectHelper): LinearRegressionModel = {
    new LinearRegressionModel(dbObject.asString("uid"),
      Vectors.dense(dbObject.asList[Double]("weights").toArray),
      dbObject.asDouble("intercept")
    )
  }
}

/**
 * A smarter Linear regression which caches old models in the model database and looks them up before
 * trying to retrain. Make sure to to call the setDb method to give it a model database.
 */
class WahooLinearRegression extends LinearRegression
with HasModelDb with CanCache[LinearRegressionModel] {
  override def modelSpec(dataset: DataFrame): LinearRegressionSpec =
    new LinearRegressionSpec(dataset.columns, super.getRegParam, super.getMaxIter)
}
