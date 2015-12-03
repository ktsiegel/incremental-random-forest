package org.apache.spark.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.DataFrame
import com.mongodb.casbah.Imports._
import org.apache.spark.ml.util._
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

  override def toString(): String = {
    val featureString = features.mkString("[", ", ", "]")
    s"LogisticRegression(features=$featureString, regParam=$regParam, maxIter=$maxIter)"
  }
}

/**
 * A smarter logistic regression which caches old models in the model database and looks them up before
 * trying to retrain. Make sure to to call the setDb method to give it a model database.
 */
class WahooLogisticRegression(uid: String, wc: WahooContext) extends LogisticRegression(uid)
with HasModelDb with CanCache[LogisticRegressionModel]
with MultiThreadTrain[LogisticRegressionModel] {
  this.setDb(wc.modelDB) // TODO: may go away if we change the cancache traits

  override def train(dataset: DataFrame): LogisticRegressionModel = {
    wc.log_msg(s"Running model $uid")
    val ms = modelSpec(dataset).toString
    wc.log_msg(s"ModelSpec: $ms")
    val model = super.train(dataset)
    wc.log_msg(s"Training complete")
    val summary = model.summary
    val numIter = summary.totalIterations
    val objhist = summary.objectiveHistory.mkString("[", ", ", "]")
    wc.log_msg(s"Objective History: $objhist")
    wc.log_msg(s"# Iterations: $numIter")
    wc.log_msg(s"Finished model $uid")
    model
  }

  def this(wc: WahooContext) = this(Identifiable.randomUID("logreg"), wc)
  override def modelSpec(dataset: DataFrame): LogisticRegressionSpec =
    new LogisticRegressionSpec(dataset.columns, super.getRegParam, super.getMaxIter)
}
