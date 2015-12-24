package org.apache.spark.ml

import scala.collection.mutable.ArrayBuffer
import com.mongodb.casbah.Imports._
import org.apache.spark.ml.util._

class WahooLog(wc: WahooContext) {
  val uuid = java.util.UUID.randomUUID.toString
  val messages: ArrayBuffer[String] = new ArrayBuffer[String]()

  def addMessage(message: String) = {
    messages += message
    wc.log_msg(message)
  }

  def toDBObject: MongoDBObject = DBObject("uid" -> uuid, "messages" -> messages)
}
