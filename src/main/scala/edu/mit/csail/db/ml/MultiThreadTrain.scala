package org.apache.spark.ml

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._

/**
  * Trait that supports training models simultaneously using multiple threads.
  */
trait MultiThreadTrain[M <: Model[M]] extends Estimator[M] {

  /**
    * Number of threads to use in training. Default is 3.
    */
  def numThreads: Int = 3

  /**
    * The timeout to wait until giving up on the model training. Default is 1 hour.
    */
  def timeout: FiniteDuration = 60.minutes
  abstract override def fit(dataset: DataFrame, paramMaps: Array[ParamMap]): Seq[M] = {

    val mapsPerThread = paramMaps.length / numThreads

    val mapSplits: IndexedSeq[Array[ParamMap]] =
      if (numThreads > paramMaps.length)
        paramMaps.map((pm) => Array(pm))
      else
        paramMaps.grouped(mapsPerThread).toIndexedSeq

    val tasks: Seq[Future[Seq[M]]] = mapSplits.map((pms) => Future {
      super.fit(dataset, pms)
    })

    val aggregated: Future[Seq[Seq[M]]] = Future.sequence(tasks)

    val nonFlatResult: Seq[Seq[M]] = Await.result(aggregated, timeout)
    nonFlatResult.flatMap((s) => s)
  }
}