package org.apache.spark.ml

import org.apache.spark.SparkContext
import scalaj.http.Http

/**
 * Main entry point for Wahoo functionality. It is used to initialize the modelDB, start the UI
 * server and set up other state
 *
 * @param sc SparkContext to be used by Wahoo to execute jobs
 */
class WahooContext (sc: SparkContext, wc: WahooConfig) {
  // set up modelDB
  var modelDB = new ModelDb(
    wc.get(WahooConfig.WahooDbName, WahooConfig.WahooDefaultDbName),
    wc.getInt(WahooConfig.WahooDbPort, WahooConfig.WahooDefaultDbPort)
  )

  // TODO: Discuss what to do with the WahooUI.
  /*
  var wahooUI = new WahooUI(
    wc.getInt(WahooConfig.WahooUiPort, WahooConfig.WahooDefaultUiPort), this)
  wahooUI.start()
  */

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
  def log(data: Seq[(String, String)]) =  {
    val resp = Http(wc.get(WahooConfig.WahooNodeJsName, 
      WahooConfig.WahooNodeJsServerUrl)).postForm(data).asString
    println("resp is ", resp)
  }

  def log_msg(msg: String) = log(Seq("message" -> msg))

  // factory methods
  // TODO: make setDb chain like the rest. Related to issue #35
  def createLogisticRegression: WahooLogisticRegression = {
    new WahooLogisticRegression(this)
  }

  def createLinearRegression: WahooLinearRegression = {
    new WahooLinearRegression(this)
  }
}
