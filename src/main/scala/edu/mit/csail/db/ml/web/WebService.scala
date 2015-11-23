package org.apache.spark.ml

import akka.http.scaladsl.server.Route

trait WebService {

  def route: Route

}
