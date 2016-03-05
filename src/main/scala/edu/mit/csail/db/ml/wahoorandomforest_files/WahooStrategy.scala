package org.apache.spark.ml.wahoo

/**
  * Created by kathrynsiegel on 2/23/16.
  */
sealed trait Strategy
case object OnlineStrategy extends Strategy
case object BatchedStrategy extends Strategy
case object RandomReplacementStrategy extends Strategy

class WahooStrategy(val erf: Boolean, val strategy: Strategy) extends Serializable {
  def isIncremental = (strategy == OnlineStrategy) ||
    (strategy == BatchedStrategy)
}
