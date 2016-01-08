package org.apache.spark.ml

import org.apache.spark.SparkContext
import scalaj.http.{HttpResponse, Http}

/**
 * Main entry point for Wahoo functionality. It is used to initialize the modelDB, start the UI
 * server and set up other state
 *
 * @param sc SparkContext to be used by Wahoo to execute jobs
 */
class WahooContext (sc: SparkContext,
                    var wc: WahooConfig) {
  // set up modelDB
  var modelDB = new ModelDb(
    wc.getString(WahooConfig.DbName.paramName, WahooConfig.DbName.defaultValue),
    wc.getInt(WahooConfig.DbPort.paramName, WahooConfig.DbPort.defaultValue),
    wc.getBoolean(WahooConfig.DropFirst.paramName, WahooConfig.DropFirst.defaultValue)
  )

  // Exception class.
  class LoggingFailedException(message: String) extends Exception(message)

  /**
    * Delete the entire database.
    */
  def dropDb() = modelDB.dropDatabase

  /**
   * POSTs the data to the central Node.js server.
   *
   * @param data The sequence of pairs to POST to the server. It will be turned into a 
   * JSON structure.
   */
  def log(data: Seq[(String, String)]) = wc.get(WahooConfig.WebAppUrl.paramName) match {
    case Some(url) => {
      val response = Http(url).postForm(data).asString
      if (response.isError &&
        wc.getBoolean(WahooConfig.LoggingErrorsFatal.paramName,
          WahooConfig.LoggingErrorsFatal.defaultValue)) {
        throw new LoggingFailedException("Failed to log message to server, status code: " +
          response.statusLine)
      }
    }
    case None => {} 
  }

  /**
   * Formats log message before it will be sent to the central Node.js server.
   * @param msg The message passed by the Spark job.
   */
  def log_msg(msg: String) = {
    val appId = sc.applicationId
    log(Seq("message" -> s"$appId: $msg"))
  }

  // factory methods
  // TODO: make setDb chain like the rest. Related to issue #35
  def createLogisticRegression: WahooLogisticRegression = {
    new WahooLogisticRegression(this)
  }

  def createLinearRegression: WahooLinearRegression = {
    new WahooLinearRegression(this)
  }
}
