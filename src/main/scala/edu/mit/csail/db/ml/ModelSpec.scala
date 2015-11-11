package edu.mit.csail.db.ml

import org.apache.spark.ml.Model
import org.apache.spark.sql.DataFrame
import com.mongodb.casbah.Imports._

/**
 * A specification representing a model to be trained. Subclass this for different models and make
 * sure to implement equals(), hashCode(), toDBObject(), and toDBQuery()
 * @param features - The features to use in training.
 * @tparam M - The type of the model. For example, LogisticRegressionModel.
 */
abstract class ModelSpec[M <: Model[M]](val features: Array[String]) {

  override def equals(o: Any): Boolean = o match {
    case that: ModelSpec[M] => features.deep == that.features.deep
    case _ => false
  }

  override def hashCode: Int = features.foldLeft[Int](0)(_ + _.hashCode())

  def toDBObject(model: M): MongoDBObject

  def toDBQuery(): MongoDBObject

  def generateModel(dbObject: DBObjectHelper): M
}
