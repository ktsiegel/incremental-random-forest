package org.apache.spark.ml

import org.eclipse.jetty.server.handler.AbstractHandler
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives
import akka.stream.ActorMaterializer

import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Flow

import scala.io.StdIn

import org.apache.spark.SparkContext



/** This class is responsible for producing a web interface for WahooML. */
class WahooUI(port: Int, wc: WahooContext) extends App {

  def getPort: Int = {
    port
  }
 
  def start(launchBrowser: Boolean = false): Unit = {
    implicit val actorSystem = ActorSystem("akka-system")
    implicit val flowMaterializer = ActorMaterializer()

    val interface = "localhost"

    import Directives._

    val echoService: Flow[Message, Message, _] = Flow[Message].map {
      case TextMessage.Strict(txt) => TextMessage("ECHO: " + txt)
        case _ => TextMessage("Message type unsupported")
    }
    val route = get {
      pathEndOrSingleSlash {
        complete("Welcome to websocket server")
      }
    } ~
    path("ws-echo") {
      get {
        handleWebsocketMessages(echoService)
      }
    }
    val binding = Http().bindAndHandle(route, interface, port)
    println(s"Server is now online at http://$interface:$port\nPress RETURN to stop...")
    StdIn.readLine()

    import actorSystem.dispatcher

    binding.flatMap(_.unbind()).onComplete(_ => actorSystem.shutdown())
    println("Server is down...")
  }
  
  // TODO
  // def log(message: String): Unit = {
  // }
}
