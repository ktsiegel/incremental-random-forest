package edu.mit.csail.db.ml.wahoo

/**
  * Created by kathrynsiegel on 2/23/16.
  */
sealed trait Strategy
case object OnlineStrategy extends Strategy
case object BatchedStrategy extends Strategy
case object RandomReplacementStrategy

private[wahoo] class WahooStrategy(val erf: Boolean, val strategy: Strategy)
