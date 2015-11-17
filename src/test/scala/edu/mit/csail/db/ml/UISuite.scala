package org.apache.spark.ml

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SQLContext, DataFrame}

import org.scalatest.FunSuite

/* Test the functionality of the web UI */
class UISuite extends FunSuite
{
	// --- values ---
	// database
    val db = new TestDb()
	
	// create Spark configuration
    val conf = new SparkConf()
		.setMaster("local[2]")
		.setAppName("spark ui test")
		.set("spark.eventLog.enabled", "true")
		.set("spark.eventLog.dir", "log")
		
	// create Spark & SQL contexts
	val sc = new SparkContext(conf)
	val sqlContext = SQLContext.getOrCreate(sc)
	
	// training data
    val trainingData = sqlContext.createDataFrame(Seq(
		(1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
	
	// --- tests ---
	/* This test checks to see whether or not we can launch the Spark UI at all. */
	test("launch spark ui")
	{	
		// display Spark UI
	    val webServer = new SparkUI(sc)
		webServer.display
		
	    // train a Wahoo logistic regression model
	    val db = new TestDb()
	    db.clear()				// ONLY FOR TESTING PURPOSES
		
	    val lr = new WahooLogisticRegression()
	    lr.setMaxIter(10).setRegParam(1.0).setDb(db)
	    lr.fit(trainingData)
		
	    // training should train from scratch
	    assert(!db.fromCache)
	}
	
	/* This test checks to see whether or not we can launch the Wahoo UI at all. */
	test("launch wahoo ui")
	{
		// display Spark UI
	    val webServer = new WahooUI(8080, sc)
		webServer.display
		
	    // train a Wahoo logistic regression model
	    db.clear()				// ONLY FOR TESTING PURPOSES
		
	    val lr = new WahooLogisticRegression()
	    lr.setMaxIter(10).setRegParam(1.0).setDb(db)
	    lr.fit(trainingData)
		
	    // training should train from scratch
	    assert(!db.fromCache)
		
		// clean up
		sc.stop()
	}
}