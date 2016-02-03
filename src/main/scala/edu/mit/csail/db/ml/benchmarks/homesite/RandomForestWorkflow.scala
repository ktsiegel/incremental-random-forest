package edu.mit.csail.db.ml.benchmarks.homesite

import org.apache.spark.{SparkContext, SparkConf}

// object RandomForestWorkflow {
//   def main(args: Array[String]) {
// 
//     // read path to training data
//     val trainingDataPath = if (args.length < 1) {
//       throw new IllegalArgumentException("Missing training data file.")
//     } else {
//       args(0)
//     }
// 
//     // Set up contexts.
//     val conf = new SparkConf().setAppName("Homesite")
//     val sc = new SparkContext(conf)
//     val sqlContext = new SQLContext(sc)
// 
//     // Read the data file and convert to RDD.
//     val df = sqlContext.read
//       .format("com.databricks.spark.csv")
//       .option("header", "true") // use first line of file as header
//       .option("inferSchema", "true") // automatically infer data types
//       .load(trainingDataPath)
//       
//     // Initially, just look at numerical indicators and train a random forest
//     // model on these numbers. We must partition into int and non-int fields.
//     val (intFields, nonIntFields) = rawData.schema.partition{ (field) =>
//       field.dataType == org.apache.spark.sql.types.IntegerType
//     }
// 
//     // // Partition DataFrame into String and non-String fields
//     // val (stringFields, nonStringFields) = rawData.schema.partition{ (field) =>
//     //   field.dataType == org.apache.spark.sql.types.StringType
//     // }
//     // val stringFieldNames = stringFields.map(_.name)
//     // val nonStringFieldNames = nonStringFields.map(_.name).filter(_ != "income_level")
// 
//     // // Use StringIndexer to turn the categorical String fields into numbers.
//     // import org.apache.spark.ml.feature.StringIndexer
//     // val indexedData = stringFieldNames.map { (field) =>
//     //   new StringIndexer().setInputCol(field).setOutputCol(field + "_index")
//     // }.map { (indexer) =>
//     //   indexer.fit(rawData)
//     // }.foldLeft(rawData){ (dataFrame, transformer) =>
//     //   transformer.transform(dataFrame)
//     // }
// 
//     // Use "one hot encoder" for strings
//     // import org.apache.spark.ml.feature.OneHotEncoder
//     // val oneHotData = stringFieldNames.map { (field) =>
//     //   new OneHotEncoder().setInputCol(field + "_index").setOutputCol(field + "_vec")
//     // }.foldLeft(indexedData){ (dataFrame, transformer) =>
//     //   transformer.transform(dataFrame)
//     // }
// 
//     // // Now let's combine our features into a "features" vector.
//     // import org.apache.spark.ml.feature.VectorAssembler
//     // val assembler = new VectorAssembler()
//     //   .setInputCols((stringFieldNames.map(_ + "_vec") ++ nonStringFieldNames).toArray)
//     //   .setOutputCol("features")
//     // val almostFinalData = assembler.transform(oneHotData)
// 
//     // // One final pre-processing step: Let's use a StringIndexer on the income level.
//     // // NOTE: I don't actually think this is necessary.
//     // val finalData = new StringIndexer()
//     //   .setInputCol("income_level")
//     //   .setOutputCol("income_level_index")
//     //   .fit(almostFinalData)
//     //   .transform(almostFinalData)
// 
//     // Training/testing split.
//     val Array(training, testing) = finalData.randomSplit(Array(0.7, 0.3))
// 
//     // Train the forest.
//     import org.apache.spark.ml.classification.RandomForestClassifier
//     val rf = new RandomForestClassifier()
//       .setLabelCol("income_level_index")
//       .setFeaturesCol("features")
//       .setNumTrees(1000)
//     val forest = rf.fit(training)
// 
//     // Evaluate the forest.
//     import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//     val evaluator = new MulticlassClassificationEvaluator()
//       .setLabelCol("income_level_index")
//       .setPredictionCol("prediction")
//       .setMetricName("precision")
//     val predictions = forest.transform(testing)
//     val accuracy = evaluator.evaluate(predictions)
// 
//     // Print out the results.
//     println("Test Error = " + (1.0 - accuracy))
// 
//     // If you want to print out the random forest, uncomment the line below. Just be warned that the forest will be huge
//     // and hard to interpret.
//     // println("Learned classification forest model:\n" + forest.toDebugString)
// 
//   }
// }
