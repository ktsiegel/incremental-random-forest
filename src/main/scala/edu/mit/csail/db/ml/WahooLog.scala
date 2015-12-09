package org.apache.spark.ml

import scala.collection.mutable.ArrayBuffer
import com.mongodb.casbah.Imports._
import org.apache.spark.ml.util._

class WahooLog(wc: WahooContext) {
  val uuid = java.util.UUID.randomUUID.toString
  val messages: ArrayBuffer[String] = new ArrayBuffer[String]()

  def addMessage(msg: String, msgType:String) = {
    // construct message
    val appId = wc.sc.applicationId
    val message = "{\"appId\": \"" + appId + "\", \"msg\": " + msg + ", \"msgType\": \"" + msgType + "\" }"
    
    // add message to log
    messages += message
    
    // send message to Node
    wc.logMessage(message)
  }

  def toDBObject(): MongoDBObject = DBObject("uid" -> uuid, "messages" -> messages)
}
