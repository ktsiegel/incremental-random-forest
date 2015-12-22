package org.apache.spark.ml

import java.util.concurrent.ConcurrentHashMap
/**
 * Configuration information required by Wahoo
 * All setter methods support chaining
 *
 * Refer SparkConf.scala
 */

/**
  * Object representing configuration information required by Wahoo.
  *
  * It is based on the SparkConf from Spark.
  */
class WahooConfig () {
  import WahooConfig._

  /**
    * The configuration is stored as a map from parameter name to parameter value.
    */
  private val settings = new ConcurrentHashMap[String, String]()

  /** Set whether the database should be dropped when WahooContext connects. */
  def setDropFirst(dropFirst: Boolean) = set(DropFirst.paramName, dropFirst.toString)

  /** Set the name of the database. */
  def setDbName(dbName: String) = set(DbName.paramName, dbName)

  /** Set the port of the database. */
  def setDbPort(dbPort: String) = set(DbPort.paramName, dbPort)

  /** Set the URL of the web app that Wahoo connects to. */
  def setWebAppUrl(url: String) = set(WebAppUrl.paramName, url)

  /**
    * Set a parameter.
    * @param key - The name of the parameter.
    * @param value - The value of the parameter.
    * @return The WahooConfig object, this allows chaining of set method calls.
    */
  def set(key: String, value: String): WahooConfig = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  /**
    * Get a parameter.
    * @param key - The name of the parameter.
    * @return An Option containing the parameter value.
    */
  def get(key: String): Option[String] = Option(settings.get(key))

  /**
    * Check whether the parameter with the given name is present in the WahooConfig.
    * @param key - The name of the parameter.
    * @return Whether there is a parameter with the given name present in the WahooConfig.
    */
  def hasKey(key: String) = get(key).nonEmpty

  // Helper function for defining type-specific getter.
  private def makeGetOrElse[T](mapper: String => T) = (key: String, default: T) =>
    get(key).map((v) => mapper(v)).getOrElse(default)

  // Type specific getter functions. Each one takes two arguments (key: String, defaultValue: T)
  // and returns a T (either the parameter value or the default value).
  def getString = makeGetOrElse[String](_.toString)
  def getInt = makeGetOrElse[Int](_.toInt)
  def getBoolean = makeGetOrElse[Boolean](_.toBoolean)
  def getDouble = makeGetOrElse[Double](_.toDouble)
  def getFloat = makeGetOrElse[Float](_.toFloat)

}

/**
  * Helper class that pairs a parameter name (e.g. wahoo.dbName) with a default value.
  */
class WahooConfigParam[T](val paramName: String, val defaultValue: T)

/**
  * Companion object containing the parameters used by the WahooConfig class.
  */
object WahooConfig {
  // Whether the model database should be dropped when Wahoo connects to the database.
  val DropFirst = new WahooConfigParam[Boolean]("wahoo.dropFirst", false)

  // URL of the web app used for visualization, browsing models, etc.
  val WebAppUrl = new WahooConfigParam[String]("wahoo.webAppUrl", "localhost:3000")

  // The name of the database to connect to.
  val DbName = new WahooConfigParam[String]("wahoo.dbName", "wahooDb")

  // The port of the database.
  val DbPort = new WahooConfigParam[Int]("wahoo.dbPort", 27017)
}
