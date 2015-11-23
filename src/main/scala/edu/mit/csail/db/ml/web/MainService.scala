package org.apache.spark.ml

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

object MainService extends WebService {

  override def route: Route = pathEndOrSingleSlash {
    complete("Welcome to websocket server")
  }

}
