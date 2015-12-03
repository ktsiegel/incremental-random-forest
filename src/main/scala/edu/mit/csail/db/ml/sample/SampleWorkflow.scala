package edu.mit.csail.db.ml.sample

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object SampleWorkflow {
  def main(args: Array[String]): Unit = {

    // NOTE: We have to use this boilerplate in order to load the training data.
    val conf = new SparkConf().setAppName("Sample Workflow")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Prepare training and test data.
    val dataset = sqlContext.createDataFrame(Seq(
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5)),
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5)),
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (1.0, Vectors.dense(0.0, 1.2, -0.5))
      )).toDF("label", "features")
    val splitDataset = dataset.randomSplit(Array(8, 2))
    val train = splitDataset(0)
    val test = splitDataset(1)


    // PSEUDOCODE: 	logreg = createLogRegEstimator()
    val lr = new LogisticRegression()

    // NOTE: We cannot specify the features to use nor can we specify the optimization method.
    // PSEUDOCODE: 	m1 = logreg.train(df, spec={regParam=10, tol=0.01, opt=sgd, fs=all})
    val pm = ParamMap(lr.tol -> 0.01, lr.regParam -> 10.0)
    val model = lr.fit(train, pm)

    // PSEUDOCODE: 	m1.getIterationHistory()
    println("Objective history")
    model.summary.objectiveHistory.foreach((h) => println(h))

    // NOTE: We cannot specify incremental training.
    // PSEUDOCODE: m1[] = logreg.train(df,
    //   spec={regParam=10, tol=0.01, opt=sgd, fs=all, cont_test=true, test_data=df’},
    //   short_cct=true, frac={0.1,1,10})

    // PSEUDOCODE: 	e1 = evaluator(type=binary)
    val evaluator = new BinaryClassificationEvaluator

    // PSEUDOCODE: 	e1.eval(m1, df_test)
    val predictions = model.transform(test)
    val evaluation = evaluator.setLabelCol("label")
      .setRawPredictionCol(model.getRawPredictionCol)
      .evaluate(predictions)
    println("Evaluation ", evaluation)

    // PSEUDOCODE: 	cv = cross_validator(df, folds=3)
    // PSEUDOCODE: res = cv.cv(
    //   spec={regParam=10, tol=0.01, opt=sgd, fs=all, cont_test=true, test_data=df’}})

    val lr2 = new LogisticRegression()
      .setMaxIter(10)

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr2.regParam, Array(0.1, 0.01))
      .build()
    val cv = new CrossValidator()
      .setEstimator(lr2)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)
    val cvModel = cv.fit(dataset)
  }
}
  
  
  
  
  
