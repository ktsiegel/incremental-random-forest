package org.apache.spark.ml

import java.awt.Desktop
import java.net.URI

import org.apache.spark.SparkContext

/* 
	This class is responsible for producing a web interface for WahooML. It uses Spark's web UI.
	
	Requirements:

		- want to call into a library to run the WebServer
		- user should be able to navigate to app in browser via ip address/port

 */

class SparkUI(context: SparkContext)
{	
	// This method displays a web UI for an active Spark Context
	def display(): Unit =
	{
		Desktop.getDesktop().browse(new URI("http://localhost:4040"))
	}
}