package org.apache.spark.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.FunSuite

class TestDb extends ModelDb 
{
	var fromCache: Boolean = false
  	override def getOrElse[M <: Model[M]](spec: ModelSpec[M], dataset: DataFrame)(orElse: ()=> M): M = 
	{
    	if (get(spec, dataset) != null) fromCache = true
    	super.getOrElse(spec, dataset)(orElse)
  	}
}

/** Check whether models are cached in the model database. */
class LogisticRegressionSuite extends FunSuite 
{
	// database
	val db = new TestDb()
	
	// spark configuration
    val conf = new SparkConf()
		.setMaster("local[2]")
		.setAppName("simpletest")
		.set("spark.eventLog.enabled", "true")
		.set("spark.eventLog.dir", "log")
	
	// spark & sql contexts
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

	// training data
    val training = sqlContext.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
	
	// -logistic regression test-
	test("are models cached in the model database?") 
  	{
    	// Train a Wahoo logistic regression model.
    	db.clear() // ONLY FOR TESTING PURPOSES

    	val lr = new WahooLogisticRegression()
    	lr.setMaxIter(10).setRegParam(1.0).setDb(db)
    	lr.fit(training)

    	// The first training should train from scratch.
    	assert(!db.fromCache)

    	// The second training should just read from the cache.
    	lr.fit(training)
    	assert(db.fromCache)
  	}
	
	// -spark ui test-
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
	
	// -wahoo ui test-
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
