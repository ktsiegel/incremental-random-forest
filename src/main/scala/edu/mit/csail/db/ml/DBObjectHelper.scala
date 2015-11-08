package edu.mit.csail.db.ml

import com.mongodb.casbah.Imports._

class DBObjectHelper(underlying: DBObject) {

  def asString(key: String) = underlying.as[String](key)

  def asDouble(key: String) = underlying.as[Double](key)

  def asInt(key: String) = underlying.as[Int](key)

  def asList[A](key: String) =
    (List() ++ underlying(key).asInstanceOf[BasicDBList]) map { _.asInstanceOf[A] }

  def asDoubleList(key: String) = asList[Double](key)
}

object DBObjectHelper {

  implicit def toDBObjectHelper(obj: DBObject) = new DBObjectHelper(obj)

}
