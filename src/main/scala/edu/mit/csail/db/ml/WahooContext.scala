package org.apache.spark.ml

import org.apache.spark.SparkContext
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

  // start UI
  var wahooUI = new WahooUI(
    wc.getInt(WahooConfig.WahooUiPort, WahooConfig.WahooDefaultUiPort), this)
  wahooUI.start()

  // test methods
  def resetDb: Unit = {
    modelDB.clear()
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
