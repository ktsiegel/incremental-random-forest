package org.apache.spark.ml

import java.util.concurrent.ConcurrentHashMap

/**
 * Configuration information required by Wahoo
 * All setter methods support chaining
 * Currently the type of DB is always to mongodb
 *
 * Refer SparkConf.scala
 */
class WahooConfig () {
  import WahooConfig._
  private val settings = new ConcurrentHashMap[String, String]()

  def setDbName(dbName: String): WahooConfig = {
    set(WahooDbName, dbName)
  }

  def setDbPort(dbName: String): WahooConfig = {
    set(WahooDbPort, dbName)
  }

  def setUiPort(dbName: String): WahooConfig = {
    set(WahooUiPort, dbName)
  }

  /** Set a configuration variable. */
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


  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  // TODO: implement parameterized get and rename to getOrElse
  //def get[T <: AnyVal](key: String, default: T): T = {
  //
  //}

}

// TODO: is this the best way to specify default?
object WahooConfig {
  val WahooDbName = "wahoo.dbName"
  val WahooDefaultDbName = "wahooDb"
  val WahooDbPort = "wahoo.dbPort"
  val WahooDefaultDbPort = 27017
  val WahooUiPort = "wahoo.uiPort"
  val WahooDefaultUiPort = 8089 //TODO: what is a good default?
}