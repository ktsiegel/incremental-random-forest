package edu.mit.csail.db.ml.benchmarks.homesite

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame}

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

 
object Homesite {
  def main(args: Array[String]) {
    val trainingDataPath = if (args.length < 1) {
      throw new IllegalArgumentException("Missing training data file.")
    } else {
      args(0)
    }

    // set up contexts
    val conf = new SparkConf()
      .setAppName("Homesite")
      .setMaster("local[2]")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
  
    // Read data and convert to dataframe
    val dfInitial = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true") // use first line of file as header
          .option("inferSchema", "true") // automatically infer data types
          .load(trainingDataPath)

    // Convert label to double from int
    val toDouble = udf[Double, Integer]( _.toDouble)
    var df = dfInitial.withColumn("QuoteConversion_Flag", toDouble(dfInitial("QuoteConversion_Flag")))
  
    var indexer = new StringIndexer()
      .setInputCol("QuoteConversion_Flag")
      .setOutputCol("label")

    // Split into training and testing data
    var Array(training, testing) = df.randomSplit(Array(0.7,0.3))

    // Evaluator
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("QuoteConversion_Flag")
      .setPredictionCol("prediction")
      .setMetricName("precision")
  
    // Model 1: trained on int and double fields.
    // Cast all int fields to double fields
    val numericFields = df.schema.filter(
      entry => entry.dataType == IntegerType ||
      entry.dataType == DoubleType
    )
    df.schema.foreach{ (entry) => {
      entry.dataType match {
        case IntegerType => {
          df = df.withColumn(entry.name, toDouble(df(entry.name)))
        }
        case _ => {}
      }
    }}
  
    // Create new pipeline
    var assembler = new VectorAssembler()
      .setInputCols(numericFields.map(_.name).toArray)
      .setOutputCol("features")
    var rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
    var pipeline = new Pipeline()
      .setStages(Array(assembler, indexer, rf))

    // Train and evaluate new model
    var forest = pipeline.fit(training)
    var predictions = forest.transform(testing)
    var accuracy = evaluator.evaluate(predictions)
    println("test error = " + (1.0 - accuracy))

    // Model 2 - trained using more trees
    rf.setNumTrees(100)
    predictions = forest.transform(testing)
    accuracy = evaluator.evaluate(predictions)
    println("test error = " + (1.0 - accuracy))
  
    // Model 3 - incorporate one string field and see if that increases accuracy
    var stringFields = df.schema.filter(entry => entry.dataType == StringType &&
                                        entry.name == "Field6")
    var indexers: Array[PipelineStage] = stringFields.toArray.map( (entry) =>
      new StringIndexer()
        .setInputCol(entry.name)
        .setOutputCol(entry.name + "_index")
    )
    var encoders: Array[PipelineStage] = stringFields.toArray.map( (entry) =>
      new OneHotEncoder()
        .setInputCol(entry.name + "_index")
        .setOutputCol(entry.name + "_vec")
    )
    assembler = new VectorAssembler()
      .setInputCols(numericFields.map(_.name).toArray ++ stringFields.map(_.name + "_vec"))
      .setOutputCol("features")
    rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
    var stages = (indexers :+ indexer) ++ encoders :+ assembler :+ rf
    pipeline = new Pipeline().setStages(stages)
    forest = pipeline.fit(training)
    predictions = forest.transform(testing)
    accuracy = evaluator.evaluate(predictions)
    
    println("test error = " + (1.0 - accuracy))
  
    // Model 4: Incorporate one more string field
    var stringFields = df.schema.filter(entry => entry.dataType == StringType &&
                                      (entry.name == "Field6" ||
                                       entry.name == "Field12"))
    val indexers: Array[PipelineStage] = stringFields.toArray.map( (entry) =>
      new StringIndexer()
        .setInputCol(entry.name)
        .setOutputCol(entry.name + "_index")
    )
    val encoders: Array[PipelineStage] = stringFields.toArray.map( (entry) =>
      new OneHotEncoder()
        .setInputCol(entry.name + "_index")
        .setOutputCol(entry.name + "_vec")
    )
    assembler: PipelineStage = new VectorAssembler()
      .setInputCols(numericFields.map(_.name).toArray ++ stringFields.map(_.name + "_vec"))
      .setOutputCol("features")
    rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
    var stages = (indexers :+ indexer) ++ encoders :+ assembler :+ rf
    pipeline = new Pipeline().setStages(stages)
    forest = pipeline.fit(training)
    
    predictions = forest.transform(testing)
    accuracy = evaluator.evaluate(predictions)
    
    println("test error = " + (1.0 - accuracy))

    rf.setNumTrees(100)
    forest = pipeline.fit(training)
    predictions = forest.transform(testing)
    accuracy = evaluator.evaluate(predictions)
    
    println("test error = " + (1.0 - accuracy))
  
    stringFields = df.schema.filter(entry => entry.dataType == StringType &&
                                      (entry.name == "Field6" ||
                                       entry.name == "Field12" ||
                                       entry.name == "CoverageField8" ||
                                       entry.name == "CoverageField9"))
    assembler = new VectorAssembler()
      .setInputCols(numericFields.map(_.name).toArray ++ stringFields.map(_.name + "_vec"))
      .setOutputCol("features")
    rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)
    stages = (indexers :+ indexer) ++ encoders :+ assembler :+ rf
    pipeline = new Pipeline().setStages(stages)
    forest = pipeline.fit(training)
    evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("QuoteConversion_Flag")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    predictions = forest.transform(testing)
    accuracy = evaluator.evaluate(predictions)
    
    println("test error = " + (1.0 - accuracy))

    rf.setNumTrees(100)
    forest = pipeline.fit(training)
    predictions = forest.transform(testing)
    accuracy = evaluator.evaluate(predictions)
    
    println("test error = " + (1.0 - accuracy))
  }
}
              
