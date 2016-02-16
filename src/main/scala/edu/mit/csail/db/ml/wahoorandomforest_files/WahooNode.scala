package org.apache.spark.ml.tree.impl

import java.io.IOException

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.Logging
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impl.{BaggedPoint, DTStatsAggregator, DecisionTreeMetadata,
TimeTracker}
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{SamplingUtils, XORShiftRandom}

/**
  * Created by kathrynsiegel on 2/15/16.
  */
class WahooNode(val treeInput: RDD[TreePoint],
                val rng: Random,
                val originalLabeledPointMask: Option[Array[Int]]) {
  val labeledPointMask = if (originalLabeledPointMask == None) {
    val mask = new Array[Int](treeInput.count().asInstanceOf[Int])
    for (i <- 0 to treeInput.count().asInstanceOf[Int]) {
      mask(i) = if (rng.nextFloat() < 0.7) 1 else 0
    }
    Some(mask)
  } else {
    Some(originalLabeledPointMask)
  }

}

