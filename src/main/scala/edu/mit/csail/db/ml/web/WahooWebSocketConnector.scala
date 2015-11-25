package org.apache.spark.ml

import java.awt.Desktop
import java.io.IOException
import java.net.URI
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import javax.servlet.ServletException

import scala.collection.mutable.StringBuilder
import scala.sys.process._

import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api.ContentResponse

import org.apache.spark.SparkContext


/** 
 * TODO: This class will contain a jetty server that can connect to web socket.
 *
 * Currently, it just launches a web server with the ability to server requests and send requests.
 */
class WahooWebSocketConnector(val port: Int, wc: WahooContext) {
  /**
   * An HTTP client for sending requests.
   */
  val client: HttpClient = new HttpClient()
  client.start()

  /** 
   * This class serves a Hello World response 
   */
  class HelloWorldHandler extends AbstractHandler {
    @throws(classOf[IOException])
    @throws(classOf[ServletException])
    override def handle(
    	target: String, 
    	baseRequest: Request, 
    	request: HttpServletRequest,
        response: HttpServletResponse): Unit = {
      response.setContentType("text/plain;charset=utf-8")
      response.setStatus(HttpServletResponse.SC_OK)
      baseRequest.setHandled(true)
      response.getWriter().print("Hello world!")
    }
  }
  
  /**
   * Starts a server on a given port.
   */
  def start(launchBrowser: Boolean = false): Unit = {
    val server = new Server(port)
    server.setHandler(new HelloWorldHandler())
    server.start()
  }
}
