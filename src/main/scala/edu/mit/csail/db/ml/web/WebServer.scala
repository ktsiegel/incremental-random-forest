package edu.mit.csail.db.ml.web

import org.apache.spark.SparkContext
import org.eclipse.jetty.server.Server

/* 
	This class is responsible for producing a web interface for WahooML
	
	Requirements:

		- want to call into a library to run the WebServer
		- user should be able to navigate to app in browser via ip address/port

 */

class WebServer(port: Int, context: SparkContext)
{
  require(port >= 0)

  val server = new Server(port)
  server.start()
  server.join()

  // if no port is provided, pick a reasonable default value
  def this(context: SparkContext) = this(8080, context)
}