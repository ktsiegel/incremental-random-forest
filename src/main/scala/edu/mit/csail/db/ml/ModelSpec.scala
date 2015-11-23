package org.apache.spark.ml

import org.apache.spark.sql.DataFrame
import com.mongodb.casbah.Imports._

/**
 * A specification representing a model to be trained. Subclass this for different models and make
 * sure to implement equals(), hashCode(), toDBObject(), and toDBQuery()
 * @param features - The features to use in training.
 * @tparam M - The type of the model. For example, LogisticRegressionModel.
 */
abstract class ModelSpec[M <: Model[M]](val features: Array[String]) {

  /**
   * Determines whether two ModelSpecs are equivalent.
   * @param o - the other object with which equivalence is tested.
   * @return True if the two objects are equal, and false if not.
   */
  override def equals(o: Any): Boolean = o match {
    case that: ModelSpec[M] => features.deep == that.features.deep
    case _ => false
  }

  /**
   * Calculates the hash corresponding to this ModelSpec.
   * @return the hash
   */
  override def hashCode: Int = features.foldLeft[Int](0)(_ + _.hashCode())

  /**
   * Creates the database object corresponding to a Model.
   * This DB object is the thing that will be inserted into the DB
   * @param model - the Model that will be converted into a DB object.
   * @return a MongoDB object storing information about this Model and
   *         this ModelSpec.
   */
  def toDBObject(model: M): MongoDBObject

  /**
   * Creates a database query corresponding to a ModelSpec.
   * Used to query the DB for all models stemming for a certain ModelSpec.
   * @return a MongoDB object storing information about this ModelSpec.
   */
  def toDBQuery(): MongoDBObject

  /**
   * Generates a Model object from JSON data stored in a MongoDB object.
   * @param dbObject - the MongoDB object storing the data about the Model.
   * @return a Model object created from the information stored in the
   *         DB object.
   */
  def generateModel(dbObject: DBObjectHelper): M
}
