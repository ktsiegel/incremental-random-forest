package org.apache.spark.ml

import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.ServletException;

import org.apache.spark.SparkContext

import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.AbstractHandler

import scala.collection.mutable.StringBuilder


/* 
	This class is responsible for producing a web interface for WahooML
	
	Requirements:

		- want to call into a library to run the WebServer
		- user should be able to navigate to app in browser via ip address/port

 */

class WebServer(port: Int, context: SparkContext)
{
  require(port >= 0)

  /* This class serves an HTML page with a simple greeting */
  class GreetingHandler extends AbstractHandler
  {
    @throws(classOf[IOException])
    @throws(classOf[ServletException])
    override def handle(target: String, baseRequest: Request,
                        request: HttpServletRequest,
                        response: HttpServletResponse): Unit =
    {
      response.setContentType("text/html;charset=utf-8")
      response.setStatus(HttpServletResponse.SC_OK)

      baseRequest.setHandled(true)

      // create HTML response
      val sb = new StringBuilder()
      sb.append("<!DOCTYPE html>\n")
      sb.append("<html>\n")
      sb.append("  <head>\n")
      sb.append("    <title>WahooML</title>\n")
      sb.append("  </head>\n")
      sb.append("  <body>\n")
      sb.append("    <h1>" + context.appName + "</h1>\n")
      sb.append("    <p>id:" + context.applicationId + "<p>\n")
      sb.append("  </body>\n")
      sb.append("</html>")

      // serve HTML page
      val writer = response.getWriter()
      writer.print(sb.toString)
    }
  }

  // -set up web server-
  val server = new Server(port)
  server.setHandler(new GreetingHandler())

  // -automatically start the server and display the web interface in a browser-
  server.start()
  Desktop.getDesktop().browse(new URI("http://localhost:" + port))

  // if no port is provided, pick a reasonable default value
  def this(context: SparkContext) = this(8080, context)
}