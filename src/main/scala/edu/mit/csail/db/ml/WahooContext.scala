package org.apache.spark.ml

import org.apache.spark.SparkContext
import scalaj.http.Http

/**
 * Main entry point for Wahoo functionality. It is used to initialize the modelDB, start the UI
 * server and set up other state
 *
 * @param sc SparkContext to be used by Wahoo to execute jobs
 */
class WahooContext (sc: SparkContext, var wc: WahooConfig) {
  // set up modelDB
  var modelDB = new ModelDb(
    wc.get(WahooConfig.WahooDbName, WahooConfig.WahooDefaultDbName),
    wc.getInt(WahooConfig.WahooDbPort, WahooConfig.WahooDefaultDbPort)
  )

  // test methods
  def resetDb: Unit = {
    modelDB.clear()
  }

  /**
   * POSTs the data to the central Node.js server.
   *
   * @param data The sequence of pairs to POST to the server. It will be turned into a 
   * JSON structure.
   */
  def log(data: Seq[(String, String)]) = wc.getOption(WahooConfig.WahooNodeJsName) match {
    case Some(url) => Http(url).postForm(data).asString
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
