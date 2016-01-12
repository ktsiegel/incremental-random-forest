package org.apache.spark.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.scalatest.{FunSuite, BeforeAndAfter}
import org.apache.spark.ml.classification.{LogisticRegression}
import org.apache.spark.ml.param.{ParamMap, Param, ParamPair}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.ml.util._


/**
 * Counter singleton to count how many models have been trained.
 */
object Counter {
  var count = 0
  def get() = count
  def up() = count += 1
  def reset() = count = 0
}

/**
 * Logistic Regression without the SubsumeOptimization. It includes logic for counting
 * the number of models trained.
 */
class TestLRNoOptimization(uid: String) extends LogisticRegression(uid) {
  def this() = this(Identifiable.randomUID("logreg"))
  override def fit(dataset: DataFrame): LogisticRegressionModel = {
    Counter.up()
    super.fit(dataset)
  }
}

/**
 * Logistic Regression that takes advantage of the SubsumeOptimization. It also includes
 * counting logic (inherited from superclass).
 */
class TestLRWithOptimization(uid: String) extends TestLRNoOptimization(uid)
with SubsumeOptimization[LogisticRegressionModel] {
  def this() = this(Identifiable.randomUID("logreg"))
  override def subsumes(pm1: ParamMap, pm2: ParamMap): Boolean = {
    // We say that pm1 subsumes pm2 if the two maps are identical except that 
    // pm1.maxIter >= pm2.maxIter.
    
    // Get the maxIter parameter.
    def maxIterParam(pm: ParamMap)= pm.toSeq.find({(pair) =>
      pair.param.name == "maxIter" && pair.value.isInstanceOf[Int]
    }) match {
      case Some(pair) => Some((pair.param, pair.value))
      case None => None
    }

    // Get the max iter for both ParamMaps.
    val (pm1HasMaxIter: Boolean, pm1Param: Option[Any], pm1MaxIter: Int) = 
      maxIterParam(pm1) match {
        case Some((param, value)) => (true, Some(param), value)
        case None => (false, None, 0)
      }
    val (pm2HasMaxIter: Boolean, pm2Param: Option[Any], pm2MaxIter: Int) = 
      maxIterParam(pm1) match {
        case Some((param, value)) => (true, Some(param), value)
        case None => (false, None, 0)
      }

    if ((pm1HasMaxIter && !pm2HasMaxIter) || (pm2HasMaxIter && !pm1HasMaxIter)) {
      // If one ParamMap has a maxIter param while the other does not, neither ParamMap subsumes 
      // the other.
      false
    } else if (!pm1HasMaxIter && !pm2HasMaxIter) {
      // If neither ParamMap has a maxIter param, just compare them using the super.
      super.subsumes(pm1, pm2)
    } else {
      // Otherwise, create copies, set the maxIter to be 1 for both.
      val pm1Copy = pm1.copy
      val pm2Copy = pm2.copy
      pm1Copy.put(pm1Param.get.asInstanceOf[Param[Int]], 1)
      pm2Copy.put(pm2Param.get.asInstanceOf[Param[Int]], 1)

      // Now compare to check that all other Params are the same.
      super.subsumes(pm1Copy, pm2Copy) && pm1MaxIter >= pm2MaxIter
    }
  }
}

class SubsumeOptimizationSuite extends FunSuite with BeforeAndAfter {
  val training = TestBase.sqlContext.createDataFrame(Seq(
    (1.0, Vectors.dense(0.0, 1.1, 0.1)),
    (0.0, Vectors.dense(2.0, 1.0, -1.0)),
    (0.0, Vectors.dense(2.0, 1.3, 1.0)),
    (1.0, Vectors.dense(0.0, 1.2, -0.5))
  )).toDF("label", "features")

  var lr = new TestLRWithOptimization

  before {
    Counter.reset()
    lr = new TestLRWithOptimization
  }

  test("No optimization") {
    val lrNo = new TestLRNoOptimization
    lrNo.fit(
      training,
      Array[ParamMap](
        ParamMap(lrNo.maxIter -> 10, lrNo.regParam -> 1.0),
        ParamMap(lrNo.maxIter -> 10, lrNo.regParam -> 1.2),
        ParamMap(lrNo.maxIter -> 10, lrNo.regParam -> 1.0)
      )
    )
    // With no optimization, we train a model for each ParamMap.
    assert(Counter.count == 3)
  }

  test("Equal ParamMaps") {
    lr.fit(
      training,
      Array[ParamMap](
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.2),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0)
      )
    )
    // Since the first and last ParamMap are the same, we only need to train 2 models.
    assert(Counter.count == 2)
  }

  test("Nonequal ParamMaps") {
    lr.fit(
      training,
      Array[ParamMap](
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.2),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.3)
      )
    )
    // Since the ParamMaps are all different, we train three models.
    assert(Counter.count == 3)
  }

  test("Multiple equal ParamMaps") {
    lr.fit(
      training,
      Array[ParamMap](
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.2),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0)
      )
    )
    // Since four ParamMaps are the same, we only need to train 2 models.
    assert(Counter.count == 2)
  }

  test("One subsumed") {
    lr.fit(
      training,
      Array[ParamMap](
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.2),
        ParamMap(lr.maxIter -> 12, lr.regParam -> 1.0)
      )
    )
    // Since the last ParamMap is the same as the first, but is trained for more iterations, we only
    // need to train one of them.
    assert(Counter.count == 2)
  }

  test("Multiple subsumed") {
    lr.fit(
      training,
      Array[ParamMap](
        ParamMap(lr.maxIter -> 3, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 12, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.0),
        ParamMap(lr.maxIter -> 10, lr.regParam -> 1.2),
        ParamMap(lr.maxIter -> 8, lr.regParam -> 1.0)
      )
    )
    // Four ParamMaps have regParam = 1.0, so we just train the one with the highest maxIter.
    assert(Counter.count == 2)
  }
}
