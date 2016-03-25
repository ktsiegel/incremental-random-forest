package org.apache.spark.ml.wahoo

/**
  * Created by kathrynsiegel on 2/23/16.
  */
sealed trait Strategy

/**
  * Control: Train a new RF from scratch on the aggregate data
  * (all data seen previously + the new batch of data).
  */
case object DefaultStrategy extends Strategy

/**
  * Incremental Growth by Gini Impurity: Each new batch of points
  * is used to grow the existing RF to a greater depth than
  * previously specified. Across all batches, leaves collect
  * aggregate statistics until they split, and then they start
  * collecting aggregate statistics from scratch. This strategy
  * never regrows the trees from the base; the additional batches
  * of data are used to split leaves in the decision trees that
  * compose the RF.
  */
case object BatchedStrategy extends Strategy

/**
  * Incremental trees with random replacement: With every new
  * batch, replace a proportional number of trees with trees
  * trained on the new batch. We determine the number of trees
  * that are replaced by calculating the ratio of the size of
  * the new batch to the number of points already seen. We then
  * replace the corresponding fraction of trees.
  * Also, weight trees according to two metrics:
  *   1) With every new batch, add a few new trees grown on only
  *   the new dataset. Weight each individual decision tree's vote
  *   in the RF based on the number of additional batches the RF
  *   has seen after the decision tree was grown. "Older" trees
  *   (in number of batches passed) should be weighted less than
  *   younger trees. After the RF has seen several batches, the
  *   oldest trees should begin to be removed from the RF.
  *   2) With every new batch, reweight the existing trees
  *   based on each tree's accuracy on this new batch of data.
  */
case object RandomReplacementStrategy extends Strategy

case object CombinedStrategy extends Strategy

class WahooStrategy(val erf: Boolean, val strategy: Strategy) extends Serializable {
  def isIncremental = true
}
