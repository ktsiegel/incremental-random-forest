package org.apache.spark.ml

import java.awt.Desktop
import java.net.URI

import org.scalatest.{FunSuite, BeforeAndAfter}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}

/** Check whether UI displays at all. */
class UISuite extends FunSuite with BeforeAndAfter {
  before {
    TestBase.wcontext.resetDb
  }

  // display Wahoo UI
  test("launch spark ui") {
    Desktop.getDesktop().browse(new URI("http://localhost:" +
      TestBase.wcontext.wahooUI.getPort))
  }
}
