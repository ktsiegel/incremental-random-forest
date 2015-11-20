package org.apache.spark.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame
import com.mongodb.casbah.Imports._
import org.apache.spark.mllib.linalg.{Vector, Vectors}

//TODO: Add more fields to this specification.
/**
 * A specification representing a logistic regression to train. It should include anything that would
 * be necessary for training a logistic regression model.
 * @param features - The features to use in training.
 * @param regParam - The regularization parameter.
 */
class LogisticRegressionSpec(override val features: Array[String], val regParam: Double, val maxIter: Int)
  extends ModelSpec[LogisticRegressionModel](features) {

  override def equals(o: Any): Boolean = o match {
    case that: LogisticRegressionSpec => super.equals(o) && that.regParam == regParam
    case _ => false
  }

  override def hashCode(): Int = super.hashCode() + regParam.hashCode()

  override def toDBObject(model: LogisticRegressionModel): MongoDBObject =
    DBObject(
      "uid" -> model.uid,
      "weights" -> model.weights.toArray,
      "intercept" -> model.intercept,
      "modelspec" -> DBObject(
        "type" -> "LogisticRegressionModel",
        "features" -> features,
        "regParam" -> regParam,
        "maxIter" -> maxIter
      )
    )

  override def toDBQuery(): MongoDBObject = 
    DBObject("modelspec" -> DBObject(
      "type" -> "LogisticRegressionModel",
      "features" -> features,
      "regParam" -> regParam,
      "maxIter" -> maxIter
    ))

  override def generateModel(dbObject: DBObjectHelper): LogisticRegressionModel = {
    new LogisticRegressionModel(dbObject.asString("uid"),
      Vectors.dense(dbObject.asList[Double]("weights").toArray),
      dbObject.asDouble("intercept")
    )
  }
}

