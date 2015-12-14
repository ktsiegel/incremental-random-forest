package org.apache.spark.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.{FunSuite, BeforeAndAfter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.classification.{WahooLogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
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
      .agg(first("label") as "label", sumVector($"featureVector") as "feature")
      .select("label", "feature")

    val labelIndexer = new StringIndexer()
      .setInputCol("label").setOutputCol("labelIndex").fit(aggregateFeatures)
      labelIndexer.transform(aggregateFeatures).select("labelIndex", "feature").persist(StorageLevel.MEMORY_ONLY_SER)
  }
}


/**
 * Check whether models are cached in the model database.
 */
class IncrementalTrainingSuite extends FunSuite with BeforeAndAfter {
  before {
    TestBase.wcontext.resetDb
  }

  /**
   * Dataset source: UCI Machine Learning Repository
   */
//   test("incremental training works") {
//     var i = 0
//     for (i <- 1 to 10) {
//       val data = TestBase.sqlContext.read
//       .format("com.databricks.spark.csv")
//       .option("header","true")
//       .option("inferSchema","true")
//       .load("src/test/scala/edu/mit/csail/db/ml/data/test.csv")
// 
//       val toVec = udf[Vector, Int, Double, Double, Double, Double, Int] {
//         (a,b,c,d,e,f) => Vectors.dense(a,b,c,d,e,f)
//       }
//       val toBinary = udf[Double, Double]( a => {
//         if (a > 30.2) 1 // 2011 CAFE standard
//         else 0
//       })
//       val allData = data.withColumn("features", toVec(
//         data("cylinders"),
//         data("displacement"),
//         data("horsepower"),
//         data("weight"),
//         data("acceleration"),
//         data("year")
//       )).withColumn("label", toBinary(data("mpg")))
//         .select("features","label")
//         .randomSplit(Array(0.7,0.1,0.2))
// 
//       val training = allData(0)
//       val incremental = allData(1)
//       val testing = allData(2)
// 
//       val lr = TestBase.wcontext.createLogisticRegression
//       lr.setRegParam(0.05)
// 
//       // Incremental iterations (control): Training for 5 iterations, then
//       // training for 10 iterations.
//       val model = time {
//         lr.setMaxIter(10)
//         lr.fit(training)
//         lr.setMaxIter(20)
//         lr.fit(training)
//       }
//       // println("Accuracy of control 1: " + evalModel(model, testing))
//       
//       // Incremental iterations (experimental) Training for 5 iterations, and
//       // then using a warm start to train for 5 more iterations
//       val modelMoreIter = time {
//         lr.setMaxIter(10)
//         val modelPreMoreIter = lr.fit(training)
//         lr.train(model, training, 10)
//       }
//       // println("Accuracy of exp 1: " + evalModel(modelMoreIter, testing))
// 
//       // Incremental data (control): Training for 10 iterations, then training
//       // for 10 iterations on a larger data set
//       val model2 = time {
//         lr.setMaxIter(10)
//         lr.fit(training)
//         val moreData: DataFrame = training.unionAll(incremental)
//         lr.fit(moreData)
//       }
//       // println("Accuracy of control 2: " + evalModel(model2, testing))
// 
//       // Incremental data (experimental): training for 10 iterations, then
//       // training for 5 iterations on the larger dataset
//       val modelMoreData = time {
//         lr.setMaxIter(10)
//         lr.fit(training)
//         val moreData2: DataFrame = training.unionAll(incremental)
//         lr.train(model, moreData2, 5)
//       }
//       // println("Accuracy of exp 2: " + evalModel(modelMoreData, testing))
// 
//       println("\n" + evalModel(model, testing) + "\t" + evalModel(modelMoreIter, testing) + "\t" + evalModel(model2, testing) + "\t" + evalModel(modelMoreData, testing))
//     }
//   }

  /**
   * Dataset source: Kaggle "What's Cooking" challenge
   * https://www.kaggle.com/c/whats-cooking/data
   */
  test("incremental training works 2") {
    val pathToDataFile = "/Users/kathrynsiegel/Development/ml-project/src/test/scala/edu/mit/csail/db/ml/data/wctrain.json"

    val recipes = Some(TestBase.sqlContext.read.json(pathToDataFile))
    val dataset = Some(sumVector.preprocess(recipes.get, TestBase.sqlContext))
    val splits = dataset.get.randomSplit(Array(0.7, 0.1, 0.2))
    val trainingSome = Some(splits(0))
    val incrementalSome = Some(splits(1))
    val testingSome = Some(splits(2))

    val training: DataFrame = trainingSome.get.select("feature", "labelIndex")
      .where(dataset.get("labelIndex") < 2)
    val incremental: DataFrame = incrementalSome.get.select("feature", "labelIndex")
      .where(dataset.get("labelIndex") < 2)
    val testing: DataFrame = testingSome.get.select("feature", "labelIndex")
      .where(dataset.get("labelIndex") < 2)

    val lr = TestBase.wcontext.createLogisticRegression
    lr.setRegParam(0.05)

    lr.setFeaturesCol("feature")
      .setLabelCol("labelIndex")
      .setPredictionCol("predict")

    // Incremental iterations (control): Training for 5 iterations, then
    // training for 10 iterations.
    val model = time {
      lr.setMaxIter(10)
      lr.fit(training)
      lr.setMaxIter(20)
      lr.fit(training)
    }
    // println("Accuracy of control 1: " + evalModel(model, testing))
    
    // Incremental iterations (experimental) Training for 5 iterations, and
    // then using a warm start to train for 5 more iterations
    val modelMoreIter = time {
      lr.setMaxIter(10)
      val modelPreMoreIter = lr.fit(training)
      lr.train(model, training, 10)
    }
    // println("Accuracy of exp 1: " + evalModel(modelMoreIter, testing))

    // Incremental data (control): Training for 10 iterations, then training
    // for 10 iterations on a larger data set
    val model2 = time {
      lr.setMaxIter(10)
      lr.fit(training)
      val moreData: DataFrame = training.unionAll(incremental)
      lr.fit(moreData)
    }
    // println("Accuracy of control 2: " + evalModel(model2, testing))

    // Incremental data (experimental): training for 10 iterations, then
    // training for 5 iterations on the larger dataset
    val modelMoreData = time {
      lr.setMaxIter(10)
      lr.fit(training)
      val moreData2: DataFrame = training.unionAll(incremental)
      lr.train(model, moreData2, 5)
    }
    // println("Accuracy of exp 2: " + evalModel(modelMoreData, testing))

    println("\n" + evalModel(model, testing) + "\t" + evalModel(modelMoreIter, testing) + "\t" + evalModel(model2, testing) + "\t" + evalModel(modelMoreData, testing))
  }


  /**
   * This method evaluates a model on a test data set and outputs both
   * the results and the overall accuracy. This method exists within
   * the LogisticRegressionModel class, but is private, so this is
   * a modified version for testing purposes.
   * @param model - the trained model
   * @param testing - the testing data
   * @return the accuracy of the model on the testing data
   */
  def evalModel(model: LogisticRegressionModel, testing: DataFrame): Double = {
    var count = 0.0
    var pos = 0.0
    model.transform(testing)
      .select("feature", "labelIndex", "predict")
      // .select("features", "label", "prediction")
      .collect()
      .foreach { case Row(feature: Vector, labelIndex: Double, predict: Double) =>
        count += 1.0
        if (labelIndex == predict) pos += 1.0
      }
    pos/count
  }

  /**
   * This method reports the time a function's execution takes.
   * @param block - the function to be timed
   * @return the return value of the function
   */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1-t0)/1e9)
    // print((t1-t0)/1e9 + "\t")
    result
  }
}
