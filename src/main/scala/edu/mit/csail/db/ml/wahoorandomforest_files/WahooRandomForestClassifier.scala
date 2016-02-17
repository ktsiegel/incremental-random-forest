//package org.apache.spark.ml.wahoo
//
//import org.apache.spark.annotation.{Experimental, Since}
//import org.apache.spark.ml.classification.DecisionTreeClassificationModel
//import org.apache.spark.ml.param.ParamMap
//import org.apache.spark.ml.tree.{DecisionTreeModel, RandomForestParams, TreeClassifierParams, TreeEnsembleModel}
//import org.apache.spark.ml.util.{Identifiable, MetadataUtils}
//import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
//import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
//import org.apache.spark.mllib.tree.model.{RandomForestModel => OldRandomForestModel}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//// import org.apache.spark.ml.tree.impl.WahooRandomForest
//
//
///**
// * A smarter Random Forest Classifier which can add more trees to a model
// * that has already been trained. This classifier wraps the existing Random Forest
// * classifier class from Spark ML.
// */
//class WahooRandomForestClassifier(override val uid: String) extends RandomForestClassifier {
//
//  def this() = this(Identifiable.randomUID("rfc"))
//
//  /**
//   * Trains a model for future online learning by maintaining candidate splits
//   * at every node.
//    *
//    * @param dataset - the initial dataset on which the model will be trained.
//   * @return - a trained model that maintains candidate splits at nodes so that it can
//   * be updated in an online fashion.
//   */
//  override def train(dataset: DataFrame): RandomForestClassificationModel = {
//    // Extract the features labeled "features" from the dataset. We train on this
//    // column only.
//    val categoricalFeatures: Map[Int, Int] =
//      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
//
//    // Determine the number of classes for RF classification.
//    // WahooRandomForestClassifier is a binary classifier, so this number should be 2.
//    val numClasses: Int = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
//      case Some(n: Int) => {
//        if (n != 2) {
//          throw new IllegalArgumentException("WahooRandomForestClassifier was assigned a " +
//            "non-binary classification problem. The number of classes must be 2.")
//        }
//        n
//      }
//      case None => throw new IllegalArgumentException("RandomForestClassifier was given input" +
//        s" with invalid label column ${$(labelCol)}, without the number of classes" +
//        " specified. See StringIndexer.")
//    }
//
//    // Extract label and features column from dataset and place into RDD
//    val oldDataset: RDD[LabeledPoint] = extractLabeledPoints(dataset)
//
//    // Use classification
//    val strategy =
//      super.getOldStrategy(categoricalFeatures, numClasses, OldAlgo.Classification, getOldImpurity)
//
//    // val trees =
//      // WahooRandomForest.run(oldDataset, strategy, getNumTrees, getFeatureSubsetStrategy, getSeed)
//        // .map(_.asInstanceOf[DecisionTreeClassificationModel])
//
//    // val numFeatures = oldDataset.first().features.size
//    val trees = new Array[DecisionTreeClassificationModel](0)
//    val numFeatures = 0
//    new RandomForestClassificationModel(trees, numFeatures, numClasses)
//  }
//
//  /**
//   * Trains a model using a warm start, adding more decision trees to the
//   * existing set of decision trees within the model.
//    *
//    * @param oldModel - the trained model that will be trained further
//   * @param dataset - the dataset on which the model will be trained
//   * @param addedTrees - the number of additional decision trees.
//   * @return an updated model that incorporates the new trees.
//   */
//  def addTrees(oldModel: RandomForestClassificationModel, dataset: DataFrame, addedTrees: Int): RandomForestClassificationModel = {
//    super.setNumTrees(addedTrees)
//    val model = super.train(dataset)
//    val trees: Array[DecisionTreeClassificationModel] = (oldModel.trees ++ model.trees).map(_.asInstanceOf[DecisionTreeClassificationModel])
//    new RandomForestClassificationModel(oldModel.uid, trees, oldModel.numFeatures, oldModel.numClasses)
//  }
//}
