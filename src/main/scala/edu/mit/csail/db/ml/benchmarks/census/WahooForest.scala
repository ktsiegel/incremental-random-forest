//package edu.mit.csail.db.ml.benchmarks.census
//
//import org.apache.spark.ml.wahooo.WahooRandomForestClassifier
//import org.apache.spark.ml.{WahooConfig, WahooContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Build a Random Forest to examine demographic data (from census) and predict whether a person makes more or less than
//  * $50K.
//  */
//object WahooForest {
//  def main(args: Array[String]) {
//    // Read the positional argument (i.e. the path to the data file).
//    val pathToDataFile = if (args.length < 1) {
//      throw new IllegalArgumentException("Missing path to data file positional argument")
//    } else {
//      args(0)
//    }
//
//    // Set up contexts.
//    import org.apache.spark.{SparkContext, SparkConf}
//    val conf = new SparkConf().setAppName("Census")
//    val sc = new SparkContext(conf)
//    val wahooConfig = new WahooConfig()
//    val wahooContext = new WahooContext(sc, wahooConfig)
//
//    // Read the data file.
//    val rawCsv = sc.textFile(pathToDataFile)
//
//    // Create an RDD[Person] from the data.
//    val rows = rawCsv
//      .filter(_.length > 5) // Remove empty lines. The choice of 5 is arbitrary.
//      .map { (line) =>
//      // Split the line on the commas.
//      // Then, partition it into Array(Array(feature1, feature2, ..., feature14), Array(label)).
//      val splitted = line.split(",").splitAt(14)
//
//      // Figure out what the label is, then assign 0.0 to represent an income less than $50K and 1.0 to represent an income
//      // above $50K.
//      val labelRaw = splitted._2(0).trim
//      val label = if (labelRaw == "<=50K") 0.0
//      else if (labelRaw == ">50K") 1.0
//      else throw new IllegalArgumentException("Invalid Label " + labelRaw)
//
//      // Now, trim each of the features.
//      val f = splitted._1.map(_.trim)
//
//      // Create the person object (and convert features to integers where appropriate).
//      new Person(
//        f(0).toInt,
//        f(1),
//        f(2).toInt,
//        f(3),
//        f(4).toInt,
//        f(5),
//        f(6),
//        f(7),
//        f(8),
//        f(9),
//        f(10).toInt,
//        f(11).toInt,
//        f(12).toInt,
//        f(13),
//        label
//      )
//    }
//
//    // Let's turn the RDD[Person] into a DataFrame.
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
//    val rawData = rows.toDF()
//
//    // Now let's process the features into something a decision tree can use.
//
//    // To do that, let's first see which features are strings and which are not (we ignore "income_level" because it's
//    // the label).
//    val (stringFields, nonStringFields) = rawData.schema.partition{ (field) =>
//      field.dataType == org.apache.spark.sql.types.StringType
//    }
//    val stringFieldNames = stringFields.map(_.name)
//    val nonStringFieldNames = nonStringFields.map(_.name).filter(_ != "income_level")
//
//    // At this point, we're going to use StringIndexer to turn the categorical String fields into numbers.
//    // We'll do this by creating a StringIndexer for each String field, fit each indexer, and then sequentially
//    // transform the dataset with each indexer until we get an "indexed" dataset.
//    import org.apache.spark.ml.feature.StringIndexer
//    val indexedData = stringFieldNames.map { (field) =>
//      new StringIndexer().setInputCol(field).setOutputCol(field + "_index")
//    }.map { (indexer) =>
//      indexer.fit(rawData)
//    }.foldLeft(rawData){ (dataFrame, transformer) =>
//      transformer.transform(dataFrame)
//    }
//
//    // Representing the categorical `String` fields using numbers won't be enough, because the decision tree won't be
//    // able to split features freely. For example, suppose that "education" is either `"High School"`, `"College"`,
//    // or `"Grad School"`. If these were indexed using `0 = "High School"`, `1 = "College"`, and `2 = "Grad School"`,
//    // then it would be impossible to create a split such that `"High School"` and `"Grad School"` samples fall into
//    // one branch and `"College"` samples fall into the other. That is, you can't pick an `0 <= x <= 2` such that
//    // either `0 < x AND 2 < x AND 1 > x` or `0 > x AND 2 > x AND 1 < x`.
//    //
//    // Therefore, we'll use a `OneHotEncoder` to convert these numbers into vectors. Thus, we may get something like
//    // `"High School" = [1, 0, 0]`, `"College" = [0, 1, 0]`, and `"Grad School" = [0, 0, 1]`. This way, the
//    // aforementioned split is easily doable - look at vector `[x, y, z]` and send the sample to the left branch
//    // if `y <= 0` and to the right branch if `y > 0`. This results in the `"High School"` and `"Grad School"` samples
//    // being placed in one branch while the `"College"` samples fall into the other.
//    import org.apache.spark.ml.feature.OneHotEncoder
//    val oneHotData = stringFieldNames.map { (field) =>
//      new OneHotEncoder().setInputCol(field + "_index").setOutputCol(field + "_vec")
//    }.foldLeft(indexedData){ (dataFrame, transformer) =>
//      transformer.transform(dataFrame)
//    }
//
//    // Now let's combine our features into a "features" vector.
//    import org.apache.spark.ml.feature.VectorAssembler
//    val assembler = new VectorAssembler()
//      .setInputCols((stringFieldNames.map(_ + "_vec") ++ nonStringFieldNames).toArray)
//      .setOutputCol("features")
//    val almostFinalData = assembler.transform(oneHotData)
//
//    // One final pre-processing step: Let's use a StringIndexer on the income level.
//    // NOTE: I don't actually think this is necessary.
//    val finalData = new StringIndexer()
//      .setInputCol("income_level")
//      .setOutputCol("income_level_index")
//      .fit(almostFinalData)
//      .transform(almostFinalData)
//
//    // Training/testing split.
//    val Array(training, testing) = finalData.randomSplit(Array(0.7, 0.3))
//
//    // Train the forest.
//    import org.apache.spark.ml.wahoo.WahooRandomForestClassifier
//    val rf = new WahooRandomForestClassifier(Option(wahooContext))
//      .setLabelCol("income_level_index")
//      .setFeaturesCol("features")
//      .setNumTrees(1)
//    val forest = rf.fit(training)
//
//    // Evaluate the forest.
//    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("income_level_index")
//      .setPredictionCol("prediction")
//      .setMetricName("precision")
//    val predictions = forest.transform(testing)
//    val accuracy = evaluator.evaluate(predictions)
//
//    // Print out the results.
//    println("Test Error = " + (1.0 - accuracy))
//
//    // val forest2 = rf.trainMore(forest, training, 5)
//    // val predictions2 = forest2.transform(testing)
//    // val accuracy2 = evaluator.evaluate(predictions2)
//    // println("Test Error = " + (1.0 - accuracy2))
//
//    // If you want to print out the random forest, uncomment the line below. Just be warned that the forest will be huge
//    // and hard to interpret.
//    // println("Learned classification forest model:\n" + forest.toDebugString)
//  }
//}
