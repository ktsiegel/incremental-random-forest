package edu.mit.csail.db.ml.benchmarks

/**
  * This benchmark trains some data using the What's Cooking Kaggle dataset
  * (https://www.kaggle.com/c/whats-cooking).
  *
  * This Spark program requires command line arguments. For more information about command line arguments
  * in Spark, see here: https://spark.apache.org/docs/1.5.2/submitting-applications.html
  *
  * The command line arguments are:
  * <pathToDataFile> - Make sure this file is preprocessed using scripts/preprocess_whats_cooking.js
  * <simple> - If empty, then we do the full benchmark. Otherwise, we do a simplfied benchmark.
  *
  * Here's a sample execution for the simplified benchmark:
  *
  * spark-submit --master local[4] --class "edu.mit.csail.db.ml.benchmarks.WhatsCooking" <jar> <data_file> simple
  *
  * and for the unsimplified benchmark:
  *
  * spark-submit --master local[4] --class "edu.mit.csail.db.ml.benchmarks.WhatsCooking" <jar> <data_file>
  */

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Aggregate function for summing vectors together.
  */
object sumVector extends UserDefinedAggregateFunction {
  def inputSchema = new StructType().add("feature", new VectorUDT)
  def bufferSchema = new StructType().add("aggregate", new VectorUDT)
  def dataType = new VectorUDT
  def deterministic = true

  /**
    * A one-element vector is used as initial value.
    */
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, Vectors.dense(Array(0.0)).toSparse)
  }

  /**
    * Update buffer with a row of input record.
    */
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0)) {
      val bufferedVector = buffer.getAs[Vector](0)
      val inputVector = input.getAs[Vector](0)
      val updated = if (bufferedVector.size < inputVector.size) {
        inputVector.toSparse
      } else {
        Vectors.dense(bufferedVector.toArray.zip(inputVector.toArray).map(x => x._1 + x._2)).toSparse
      }
      buffer.update(0, updated)
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    update(buffer1, buffer2)
  }

  /** Get the final result of the function */
  def evaluate(buffer: Row) = buffer.getAs[Vector](0)

  /**
    * Apply preprocessing steps on the recipes dataset.
    */
  def preprocess(dataset: DataFrame, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    val flatRecipes = dataset.select("id", "cuisine", "ingredients").flatMap {
      case Row(id: Long, label: String, features: mutable.WrappedArray[String]) =>
        features.map(x => (id, label, x)) }.toDF("id", "label", "feature")

    val indexer = new StringIndexer()
      .setInputCol("feature").setOutputCol("featureIndex").fit(flatRecipes)
    val indexed = indexer.transform(flatRecipes)

    val encoder = new OneHotEncoder()
      .setInputCol("featureIndex").setOutputCol("featureVector")
    val encoded = encoder.transform(indexed)

    val aggregateFeatures = encoded
      .select("id", "label", "featureVector")
      .groupBy("id")
      .agg(first("label") as "label", sumVector($"featureVector") as "feature")
      .select("label", "feature")

    val labelIndexer = new StringIndexer()
      .setInputCol("label").setOutputCol("labelIndex").fit(aggregateFeatures)
    labelIndexer.transform(aggregateFeatures).select("labelIndex", "feature").persist(StorageLevel.MEMORY_ONLY_SER)
  }
}

object WhatsCooking {
  def main(args: Array[String]): Unit = {
    /**
      * The path to the file that contains the training data.
      * JSON parsing in Spark is strange because it can't handle multi-line objects. That means that
      * the JSON file must contain a complete JavaScript object on each line. For example, a file
      * like this would not be allowed:
      * [
      *   {
      *     "somefield": 2,
      *     "otherfield": ["this", "is", "more", "stuff"],
      *   },
      *   {
      *     "somefield": 2,
      *     "otherfield": ["yet", "more", "content"],
      *   }
      * ]
      *
      * However, a file like this would be allowed:
      *
      * {"somefield": 2, "otherfield":  ["this", "is", "more", "stuff"]}
      * {"somefield": 2, "otherfield": ["yet", "more", "content"]}
      */
    val pathToDataFile = args(0)
    val isSimple = args.length > 1
    val conf = new SparkConf().setAppName("Cooking")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.shuffle.partitions", "10")
    conf.registerKryoClasses(Array(classOf[Text], classOf[NullWritable], classOf[VectorUDT]))

    val sc = new SparkContext(conf)
    val sqlContext  = new SQLContext(sc)
    val recipes = sqlContext.read.json(pathToDataFile)

    val dataset = sumVector.preprocess(recipes, sqlContext)
    val splits = dataset.randomSplit(Array(0.7, 0.3))

    def simplify = (df: DataFrame) => if (!isSimple) df else df.select("feature", "labelIndex")
      .where(df("labelIndex") < 2)

    val training = simplify(splits(0))
    val test = simplify(splits(1))

    val singleClassifier = new LogisticRegression().setMaxIter(3)
    val eval = new MulticlassClassificationEvaluator()
      .setLabelCol("labelIndex").setPredictionCol("predict").setMetricName("f1")

    if (isSimple) {
      singleClassifier
        .setFeaturesCol("feature")
        .setLabelCol("labelIndex")
        .setPredictionCol("predict")
      val pm1 = ParamMap(
        singleClassifier.elasticNetParam -> 1.0,
        singleClassifier.regParam -> 0.1,
        singleClassifier.fitIntercept -> true
      )
      val pm2 = ParamMap(
        singleClassifier.elasticNetParam -> 0.1,
        singleClassifier.regParam -> 0.01,
        singleClassifier.fitIntercept -> true
      )
      val pm3 = ParamMap(
        singleClassifier.elasticNetParam -> 0.01,
        singleClassifier.regParam -> 0.1,
        singleClassifier.fitIntercept -> false
      )

      val models = singleClassifier.fit(training, Array(pm1, pm2, pm3))

      val f1Scores = models.map((model) => eval.evaluate(model.transform(test)))
      println("F1 scores " + f1Scores)
    } else {
      val params = new ParamGridBuilder()
        .addGrid(singleClassifier.elasticNetParam, Array(1.0, 0.1, 0.01))
        .addGrid(singleClassifier.regParam, Array(0.1, 0.01))
        .addGrid(singleClassifier.fitIntercept, Array(true, false))
        .build()

      val multiClassifier = new OneVsRest()
        .setFeaturesCol("feature")
        .setLabelCol("labelIndex")
        .setPredictionCol("predict")
        .setClassifier(singleClassifier)

      val crossValidator = new CrossValidator()
        .setEstimator(multiClassifier).setEstimatorParamMaps(params).setNumFolds(2).setEvaluator(eval)
      val model = crossValidator.fit(training)
      val f1Score = eval.evaluate(model.transform(test))
      println("F1 score is " + f1Score)
    }
  }
}
