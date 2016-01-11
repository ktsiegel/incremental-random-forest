package org.apache.spark.ml

import java.io.FileNotFoundException

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.ml.classification.{OneVsRest, LogisticRegression}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.scalatest.{FunSuite, BeforeAndAfter}

import scala.collection.mutable

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
      .agg(first("label") as "label", Preprocessor($"featureVector") as "feature")
      .select("label", "feature")

    val labelIndexer = new StringIndexer()
      .setInputCol("label").setOutputCol("labelIndex").fit(aggregateFeatures)
    labelIndexer.transform(aggregateFeatures).select("labelIndex", "feature").persist(StorageLevel.MEMORY_ONLY_SER)
  }
}

class WhatsCookingBenchmark extends FunSuite with BeforeAndAfter {

  var train: Option[DataFrame] = None
  var test: Option[DataFrame] = None
  var dataset: Option[DataFrame] = None

  val eval = new MulticlassClassificationEvaluator()
    .setLabelCol("labelIndex").setPredictionCol("predict").setMetricName("f1")

  val singleClassifier = new LogisticRegression().setMaxIter(3)


  before {
    if (dataset.isEmpty) {
      val dataFile = new java.io.File(TestBase.WhatsCookingDataFile)
      if (!dataFile.exists()) {
        throw new FileNotFoundException("Expected to find " + TestBase.WhatsCookingDataFile
          + " in project root. Please add it and remember to preprocess it with the benchmark "
          + "script called preprocess_whats_cooking.js.")
      }
      val recipes = Some(TestBase.sqlContext.read.json(TestBase.WhatsCookingDataFile))
      dataset = Some(sumVector.preprocess(recipes.get, TestBase.sqlContext))
      val splits = dataset.get.randomSplit(Array(0.7, 0.3))
      train = Some(splits(0))
      test = Some(splits(1))
    }
  }

  test("train a few models with Spark classes") {
    TestBase.withContext { (wctx) =>
      val simpleTrain = train.get.select("feature", "labelIndex")
        .where(dataset.get("labelIndex") < 2)
      val simpleTest = test.get.select("feature", "labelIndex")
        .where(dataset.get("labelIndex") < 2)

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

      var models: Option[Seq[Transformer]] = None
      TestBase.time("training spark logistic regression") { () =>
        models = Some(singleClassifier.fit(simpleTrain, Array(pm1, pm2, pm3)))
      }

      val f1Scores = models.get.map((model) => eval.evaluate(model.transform(simpleTest)))
      println("spark f1scores " + f1Scores)
    }
  }

  test("train a few models with Wahoo classes") {
    TestBase.withContext { (wctx) =>
      val simpleClassifier = wctx.createLogisticRegression.setMaxIter(3)
      val simpleTrain = train.get.select("feature", "labelIndex")
        .where(dataset.get("labelIndex") < 2)
      val simpleTest = test.get.select("feature", "labelIndex")
        .where(dataset.get("labelIndex") < 2)

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

      var models: Option[Seq[Transformer]] = None
      TestBase.time("training wahoo logistic regression") { () =>
        models = Some(singleClassifier.fit(simpleTrain, Array(pm1, pm2, pm3)))
      }

      val f1Scores = models.get.map((model) => eval.evaluate(model.transform(simpleTest)))
      println("wahoo f1scores " + f1Scores)
    }
  }
}
