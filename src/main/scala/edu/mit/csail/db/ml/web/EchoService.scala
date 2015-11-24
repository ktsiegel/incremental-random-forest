package org.apache.spark.ml

import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Flow
import spray.json._
import DefaultJsonProtocol._

object EchoService extends WebService {
  override def route: Route = path("ws-echo") {
    get {
      handleWebsocketMessages(echoService)
    }
  }

  val echoService: Flow[Message, Message, _] = Flow[Message].map {
    case TextMessage.Strict(txt) => {
      println("received message")
      val msg = Map("echo" -> txt).toJson.compactPrint
      println("will echo back", msg)
      TextMessage(msg)
    }
    case _ => TextMessage("Message type unsupported")
  }
}
