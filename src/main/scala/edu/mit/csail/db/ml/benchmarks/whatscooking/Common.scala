package edu.mit.csail.db.ml.benchmarks.whatscooking

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.ml.{WahooConfig, WahooContext}
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Functions common to both benchmarks that use the What's Cooking dataset.
  */
object Common {
  /**
    * @param args The command line arguments
    * @return (training DataFrame, test DataFrame, WahooContext, whether Wahoo classes should be used).
    */
  def readInput(args: Array[String]) = {
    // The first positional argument is the path to the data file.
    val pathToDataFile = if (args.length < 1) {
      throw new IllegalArgumentException("Missing path to data file positional argument")
    } else {
      args(0)
    }

    // The second positional argument indicates whether Spark or Wahoo classes should be used.
    val shouldUseWahoo = if (args.length < 2) {
      throw new IllegalArgumentException("Missing [wahoo|spark] positional argument")
    } else {
      args(1) match {
        case "wahoo" => true
        case "spark" => false
        case _ => throw new IllegalArgumentException("[wahoo|spark] argument must be either \"spark\" or \"wahoo\"")
      }
    }

    // Create the configuration.
    val conf = new SparkConf().setAppName("Cooking")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.shuffle.partitions", "10")
    conf.registerKryoClasses(Array(classOf[Text], classOf[NullWritable], classOf[VectorUDT]))

    // Create the contexts.
    val sc = new SparkContext(conf)
    val sqlContext  = new SQLContext(sc)
    val wahooConfig = new WahooConfig()
    val wahooContext = new WahooContext(sc, wahooConfig)
    val recipes = sqlContext.read.json(pathToDataFile)

    // Preprocess the dataset and split it into training and testing datasets.
    val dataset = SumVector.preprocess(recipes, sqlContext)
    val splits = dataset.randomSplit(Array(0.7, 0.3))

    (splits(0), splits(1), wahooContext, shouldUseWahoo)
  }
}
