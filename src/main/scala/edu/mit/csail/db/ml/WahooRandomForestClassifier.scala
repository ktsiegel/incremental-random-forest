package org.apache.spark.ml.classification

import org.apache.spark.ml.classification.{RandomForestClassifier, RandomForestClassificationModel, DecisionTreeClassificationModel}
import org.apache.spark.ml.tree.DecisionTreeModel
import org.apache.spark.ml.{WahooContext, MultiThreadTrain}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * A smarter Random Forest Classifier which can add more trees to a model
 * that has already been trained.
 */
class WahooRandomForestClassifier(uid: String, wc: Option[WahooContext]) {
  val rf = new RandomForestClassifier(uid)

  def fit(dataset: DataFrame): RandomForestClassificationModel = rf.fit(dataset)

  def setLabelCol(label: String): this.type = {
    rf.setLabelCol(label)
    return this
  }

  def setFeaturesCol(label: String): this.type = {
    rf.setFeaturesCol(label)
    return this
  }

  def setNumTrees(num: Int): this.type = {
    rf.setNumTrees(num)
    return this
  }

  def this(wc: Option[WahooContext]) = this(Identifiable.randomUID("forest"), wc)

  /**
   * Trains a model using a warm start, adding more decision trees to the
   * existing set of decision trees within the model.
   * @param oldModel - the trained model that will be trained further
   * @param dataset - the dataset on which the model will be trained
   * @param addedTrees - the number of additional decision trees.
   * @return a trained model
   */
  def fit(oldModel: RandomForestClassificationModel, dataset: DataFrame, addedTrees: Int): RandomForestClassificationModel = {
    rf.setNumTrees(addedTrees)
    val model = rf.fit(dataset)
    val trees: Array[DecisionTreeClassificationModel] = (oldModel.trees ++ model.trees).map(_.asInstanceOf[DecisionTreeClassificationModel])
    new RandomForestClassificationModel(oldModel.uid, trees, oldModel.numFeatures, oldModel.numClasses)
  }
}
