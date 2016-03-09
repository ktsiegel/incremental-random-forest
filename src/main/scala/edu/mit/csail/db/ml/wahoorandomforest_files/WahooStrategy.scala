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
  * Online RF: Each point in each new batch of points is individually
  * used to grow the existing RF to a greater depth than
  * previously specified. As with the incremental growth strategy,
  * leaves collect aggregate statistics until they split, and
  * then start collecting aggregate statistics from scratch. Each
  * individual point is applied to potentially split leaves in the
  * decision trees that compose the RF.
  */
case object OnlineStrategy extends Strategy

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
  * Incremental Growth by Hoeffding Bounds: Each new batch of
  * points is used to grow the existing RF. Across all batches,
  * leaves collect aggregate statistics until the difference in
  * gain between the optimal split and second optimal split
  * passes the Hoeffding bound, at which point the leaf splits.
  * This strategy does not regrow the trees; instead, like with
  * the previous incremental growth method, the additional batches
  * of data are used to split more leaves in the RF.
  */
case object BatchedHoeffdingStrategy extends Strategy

/**
  * Incremental trees with random replacement: With every new
  * batch, replace a proportional number of trees with trees
  * trained on the new batch. We determine the number of trees
  * that are replaced by calculating the ratio of the size of
  * the new batch to the number of points already seen. We then
  * replace the corresponding fraction of trees.
  */
case object RandomReplacementStrategy extends Strategy

/**
  * Incremental trees with tree decay: With every new batch, add
  * a few new trees grown on only the new dataset. Weight each
  * individual decision tree’s vote in the RF based on the number
  * of additional batches the RF has seen after the decision tree
  * was grown. “Older” trees (in number of batches passed) should
  * be weighed less than younger trees. After the RF has seen
  * several batches, the oldest trees should begin to be removed
  * from the RF.
  */
case object TreeDecayStrategy extends Strategy

/**
  * Incremental trees with accuracy weighting: With every new
  * batch, reweight the existing trees based on each tree’s
  * accuracy on this new batch of data.
  */
case object TreeReweightStrategy extends Strategy

/**
  * Incremental Growth with Random Node Regeneration: Some
  * number of nodes in each decision tree is chosen to be
  * regrown using only the dataset in the new batch. We use
  * Gini impurity to detect when to split leaves during tree
  * growth.
  */
case object RandomNodeReplacementStrategy extends Strategy


class WahooStrategy(val erf: Boolean, val strategy: Strategy) extends Serializable {
  def isIncremental = (strategy == OnlineStrategy) ||
    (strategy == BatchedStrategy) ||
    (strategy == BatchedHoeffdingStrategy)
}
