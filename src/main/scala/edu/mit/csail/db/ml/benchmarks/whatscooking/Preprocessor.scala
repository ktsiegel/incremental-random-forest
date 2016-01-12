package edu.mit.csail.db.ml.benchmarks.whatscooking

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Aggregate function for summing vectors together.
  */
object Preprocessor extends UserDefinedAggregateFunction {
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
