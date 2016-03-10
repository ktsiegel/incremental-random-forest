package org.apache.spark.ml.wahoo

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.classification.ProbabilisticClassificationModel
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.wahoo.tree.Split
import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
import org.apache.spark.ml.wahoo.tree.{DecisionTreeModel, TreeEnsembleModel, DecisionTreeClassificationModel, WahooRandomForest}
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vectors, Vector}
import org.apache.spark.mllib.tree.impl.DecisionTreeMetadata
import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * A smarter Random Forest Classifier which can add more trees to a model
 * that has already been trained. This classifier wraps the existing Random Forest
 * classifier class from Spark ML.
 */
class WahooRandomForestClassifier(override val uid: String) extends RandomForestClassifier {

  this.wahooStrategy = new WahooStrategy(false, RandomReplacementStrategy)

  def this() = this(Identifiable.randomUID("rfc"))

  /**
   * Trains a model for future online learning by maintaining candidate splits
   * at every node.
   *
   * @param dataset - the initial dataset on which the model will be trained.
   * @return - a trained model that maintains candidate splits at nodes so that it can
   * be updated in an online fashion.
   */
  override def train(dataset: DataFrame): RandomForestClassificationModel = {
    // Extract the features labeled "features" from the dataset. We train on this
    // column only.
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))

    // Determine the number of classes for RF classification.
    // WahooRandomForestClassifier is a binary classifier, so this number should be 2.
    val numClasses: Int = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => {
        if (n != 2) {
          throw new IllegalArgumentException("WahooRandomForestClassifier was assigned a " +
            "non-binary classification problem. The number of classes must be 2.")
        }
        n
      }
      case None => throw new IllegalArgumentException("RandomForestClassifier was given input" +
        s" with invalid label column ${$(labelCol)}, without the number of classes" +
        " specified. See StringIndexer.")
    }

    // Extract label and features column from dataset and place into RDD
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)

    // Use classification
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity)
    val numFeatures = oldDataset.first().features.size

    val (trees, splits, metadata) =
      WahooRandomForest.run(oldDataset, strategy, getNumTrees, getFeatureSubsetStrategy,
        getSeed, wahooStrategy)

    if (wahooStrategy.isIncremental) {
      new RandomForestClassificationModel(
        trees.map(_.asInstanceOf[DecisionTreeClassificationModel]), numFeatures,
          numClasses, Some(splits), Some(metadata), wahooStrategy)
    } else {
      new RandomForestClassificationModel(
        trees.map(_.asInstanceOf[DecisionTreeClassificationModel]),
          numFeatures, numClasses, None, None, wahooStrategy)
    }
  }

  override def update(oldModel: RandomForestClassificationModel,
             dataset: DataFrame): RandomForestClassificationModel = {
    assert(wahooStrategy == oldModel.wahooStrategy,
      "New model must use the same strategy as old model.")

    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    val numClasses: Int = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) => {
        if (n != oldModel.numClasses) {
          throw new IllegalArgumentException("Error: the number of classes in a new batch " +
            "of data must match the number of classes in the previously-seen data.")
        }
        n
      }
      case None => throw new IllegalArgumentException("RandomForestClassifier was given input" +
        s" with invalid label column ${$(labelCol)}, without the number of classes" +
        " specified. See StringIndexer.")
    }
    // Extract label and features column from dataset and place into RDD
    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)

    // Use classification
    val strategy =
      super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity)

    val numFeatures = oldDataset.first().features.size
    assert(numFeatures == oldModel.numFeatures,
      "Error, the number of features in a new batch " +
        "of data must match the number of features in the previously-seen data.")

    if (wahooStrategy.isIncremental) {
      assert(oldModel.splits.isDefined && oldModel.metadata.isDefined,
    	"Error, the old model was not trained with an incremental strategy.")
      val trees = WahooRandomForest.runAndUpdateClassifier(oldModel._trees, oldDataset, strategy,
        getNumTrees, getFeatureSubsetStrategy, getSeed, wahooStrategy, oldModel.splits.get,
        oldModel.metadata.get)
        .map(_.asInstanceOf[DecisionTreeClassificationModel])

      new RandomForestClassificationModel(trees, numFeatures, numClasses,
        oldModel.splits, oldModel.metadata, wahooStrategy)
    } else {
      wahooStrategy.strategy match {
        case RandomReplacementStrategy => {
          val numNewTrees = 1
          val tempRF = new WahooRandomForestClassifier()
          tempRF.setNumTrees(numNewTrees)
          val model = tempRF.fit(dataset)
          val r = scala.util.Random
          val newTrees: ArrayBuffer[DecisionTreeClassificationModel] = new ArrayBuffer()
          Range(numNewTrees, oldModel.trees.length).map { treeIndex => {
            newTrees += oldModel._trees(treeIndex)
          }}
          Range(0, numNewTrees).map { treeIndex =>
            newTrees += model._trees(treeIndex)
          }
          new RandomForestClassificationModel(newTrees.toArray, numFeatures, numClasses,
            oldModel.splits, oldModel.metadata, wahooStrategy)
        }
        case DefaultStrategy => {
          train(dataset)
        }
        case _ => {
          throw new IllegalArgumentException("Unknown wahoo strategy.")
        }
      }
    }
  }

  /**
   * Trains a model using a warm start, adding more decision trees to the
   * existing set of decision trees within the model.
   *
   * @param oldModel - the trained model that will be trained further
   * @param dataset - the dataset on which the model will be trained
   * @param addedTrees - the number of additional decision trees.
   * @return an updated model that incorporates the new trees.
   */
  override def addTrees(oldModel: RandomForestClassificationModel, dataset: DataFrame, addedTrees: Int): RandomForestClassificationModel = {
    assert(wahooStrategy == oldModel.wahooStrategy,
      "New model must use the same strategy as old model.")
    super.setNumTrees(addedTrees)
    // TODO pass in splits and metadata as optimization
    val model = fit(dataset)
    val trees: Array[DecisionTreeClassificationModel] = (oldModel.trees ++ model.trees).map(_.asInstanceOf[DecisionTreeClassificationModel])
    new RandomForestClassificationModel(oldModel.uid, trees, oldModel.numFeatures,
      oldModel.numClasses, oldModel.splits, oldModel.metadata, oldModel.wahooStrategy)
  }
}

/**
  * :: Experimental ::
  * [[http://en.wikipedia.org/wiki/Random_forest  Random Forest]] model for classification.
  * It supports both binary and multiclass labels, as well as both continuous and categorical
  * features.
  *
  * @param _trees  Decision trees in the ensemble.
  *               Warning: These have null parents.
  * @param numFeatures  Number of features used by this model
  */
@Experimental
final class RandomForestClassificationModel private[ml] (
                                                          override val uid: String,
                                                          val _trees: Array[DecisionTreeClassificationModel],
                                                          val numFeatures: Int,
                                                          override val numClasses: Int,
                                                          val splits: Option[Array[Array[Split]]],
                                                          var metadata: Option[DecisionTreeMetadata],
                                                          val wahooStrategy: WahooStrategy)
  extends ProbabilisticClassificationModel[Vector, RandomForestClassificationModel]
    with TreeEnsembleModel with Serializable {

  require(numTrees > 0, "WahooRandomForestClassificationModel requires at least 1 tree.")

  /**
    * Construct a random forest classification model, with all trees weighted equally.
    *
    * @param trees  Component trees
    */
  private[ml] def this(
                        trees: Array[DecisionTreeClassificationModel],
                        numFeatures: Int,
                        numClasses: Int,
                        splits: Option[Array[Array[Split]]],
                        metadata: Option[DecisionTreeMetadata],
                        wahooStrategy: WahooStrategy) =
    this(Identifiable.randomUID("rfc"), trees, numFeatures, numClasses, splits, metadata,
      wahooStrategy)

  override def trees: Array[DecisionTreeModel] = _trees.asInstanceOf[Array[DecisionTreeModel]]

  // Note: We may add support for weights (based on tree performance) later on.
  private lazy val _treeWeights: Array[Double] = Array.fill[Double](numTrees)(1.0)

  override def treeWeights: Array[Double] = _treeWeights

  override protected def transformImpl(dataset: DataFrame): DataFrame = {
    val bcastModel = dataset.sqlContext.sparkContext.broadcast(this)
    val predictUDF = udf { (features: Any) =>
      bcastModel.value.predict(features.asInstanceOf[Vector])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override protected def predictRaw(features: Vector): Vector = {
    // TODO: When we add a generic Bagging class, handle transform there: SPARK-7128
    // Classifies using majority votes.
    // Ignore the tree weights since all are 1.0 for now.
    val votes = Array.fill[Double](numClasses)(0.0)
    _trees.view.foreach { tree =>
      val classCounts: Array[Double] = tree.rootNode.predictImpl(features).impurityStats.stats
      val total = classCounts.sum
      if (total != 0) {
        var i = 0
        while (i < numClasses) {
          votes(i) += classCounts(i) / total
          i += 1
        }
      }
    }
    Vectors.dense(votes)
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in WahooRandomForestClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  override def copy(extra: ParamMap): RandomForestClassificationModel = {
    copyValues(new RandomForestClassificationModel(uid, _trees, numFeatures, numClasses,
      splits, metadata, wahooStrategy),
      extra).setParent(parent)
  }

  override def toString: String = {
    s"RandomForestClassificationModel with $numTrees trees"
  }

  /**
    * Estimate of the importance of each feature.
    *
    * This generalizes the idea of "Gini" importance to other losses,
    * following the explanation of Gini importance from "Random Forests" documentation
    * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
    *
    * This feature importance is calculated as follows:
    *  - Average over trees:
    *     - importance(feature j) = sum (over nodes which split on feature j) of the gain,
    *       where gain is scaled by the number of instances passing through node
    *     - Normalize importances for tree based on total number of training instances used
    *       to build tree.
    *  - Normalize feature importance vector to sum to 1.
    */
  lazy val featureImportances: Vector = WahooRandomForest.featureImportances(trees, numFeatures)

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldRandomForestModel = {
    new OldRandomForestModel(OldAlgo.Classification, _trees.map(_.toOld))
  }
}

private[ml] object RandomForestClassificationModel {

  /** (private[ml]) Convert a model from the old API */
  def fromOld(
               oldModel: OldRandomForestModel,
               parent: RandomForestClassifier,
               categoricalFeatures: Map[Int, Int],
               numClasses: Int,
               splits: Option[Array[Array[Split]]],
               metadata: Option[DecisionTreeMetadata],
               wahooStrategy: WahooStrategy): RandomForestClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification, "Cannot convert RandomForestModel" +
      s" with algo=${oldModel.algo} (old API) to RandomForestClassificationModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeClassificationModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("rfc")
    new RandomForestClassificationModel(uid, newTrees, -1, numClasses, splits, metadata,
      wahooStrategy)
  }
}
