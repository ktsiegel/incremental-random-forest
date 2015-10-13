package edu.mit.csail.db.ml

import org.apache.spark.mllib.linalg.Vector

class TestData [T,U](val x:T, val y:U) extends Tuple2[T,U](x,y) {

}
