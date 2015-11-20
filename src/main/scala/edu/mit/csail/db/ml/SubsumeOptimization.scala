package org.apache.spark.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.param.ParamMap
import scala.annotation.tailrec

/**
 * This optimization is based on the following idea:
 * 
 * Suppose that an estimator E is fit on dataset D given two ParamMaps P1 and P2. 
 * That is, we execute:
 *
 * M1, M2 = E.fit(D, P1, P2)
 *
 * Now, suppose that P1 is identical to P2. Then there's no need to train two models, we can simply
 * train one (call it M1) and return M1, M1.
 *
 * More generally, we say that a ParamMap P1 "subsumes" another ParamMap P2 iff the model trained 
 * from P1 can be used in place of the model trained by P2. It is up to the implementer of the trait
 * to decide when this happens. For example, if P1 and P2 are identical except that P1 has a maxIter
 * that is higher than P2, then we might (the implementer must decide this) say P1 subsumes P2.
 *
 * Thus, this trait takes advantage of the "subsumes" relationship to train as few models as
 * possible and reuse them for the subsumed ParamMaps.
 *
 * For the default implementation of subsumes in this trait, we say that P1 subsumes P2 iff P1 and 
 * P2 are identical.
 */
trait SubsumeOptimization[M <: Model[M]] extends Estimator[M] {
  /**
   * Determine whether one ParamMap subsumes the other.
   *
   * The default implementation takes advantage of the fact that two ParamMaps with identical
   * ParamPairs will have the same value for toString. We say ParamMaps with the same toString
   * subsume each other.
   *
   * @param p1 - The first ParamMap.
   * @param p2 - The second ParamMap.
   * @return If p1 subsumes p2, return p1. If p2 subsumes p1, return p2. Otherwise, return None.
   */
  def subsumes(p1: ParamMap, p2: ParamMap): Boolean = p1.toString == p2.toString

  /**
   * Takes advantage of the "subsumes" relationship to train as few models as possible.
   * Specifically, we guarantee the following:
   *
   * If a P in paramMaps is not subsumed by any other ParamMap in paramMaps AND 
   * there exist {S1, S2, ..., SN} in paramMaps such that P subsumes each of them, 
   * then no model will be trained for S1, S2, ..., SN. Instead, the model trained from P will be
   * used for S1, S2, ..., SN.
   *
   * @param dataset - The DataFrame to train on.
   * @param paramMaps - The ParamMaps to train on.
   * @return The trained models, there will be one per ParamMap.
   */
  override def fit(dataset: DataFrame, paramMaps: Array[ParamMap]): Seq[M] = {
    import scala.collection.mutable.HashMap

    // 1. key = ParamMap's toString.
    // 2. value = the ParamMap, pm, such that key = pm.toString
    // 3. Each ParamMap in paramMapForString cannot be subsumed by anything in paramMaps 
    // (except itself).
    // These are the ParamMaps that we will be training models for.
    val paramMapForString = HashMap[String, ParamMap]()


    // 1. key = ParamMap's toString.
    // 2. value = a ParamMap
    // 3. If key = pm1.toString, value = pm2, that means that pm1 is subsumed by pm2. Note that pm2
    // may also be subsumed by something else.
    val subsumedBy = HashMap[String, ParamMap]()

    // We iterate through the paramMaps to bulid the two HashMaps above.
    paramMaps.foreach(paramMap => {
      paramMapForString
        .filter{p => subsumes(paramMap, p._2)} // Get all the ParamMaps subsumed by paramMap.
        .map{_._1} // Get their string representation.
        .foreach{asStr => {
          // Remove the subsumed ParamMap from the HashMap of ParamMaps to train models for.
          paramMapForString.remove(asStr)
          // Mark that this ParamMap was subsumed by paramMap.
          subsumedBy.put(asStr, paramMap)
        }}

      // Is there any ParamMap that subsumes paramMap?
      val subsumer = paramMapForString.find{p => subsumes(p._2, paramMap)}
      subsumer match {
        case Some((asStr, pm)) =>  {
          // If paramMap is subsumed, mark its subsumer.
          subsumedBy.put(paramMap.toString, pm)
        }
        case None => {
          // If nobody subsumes paramMap, add it to the HashMap of ParamMaps to train models for.
          paramMapForString.put(paramMap.toString, paramMap)
        }
      }
    })

    // Convert paramMapForString into an Array and train the models.
    val paramMapsToTrainOn: Array[ParamMap] = paramMapForString.map{p => p._2}.toArray
    val models = super.fit(dataset, paramMapsToTrainOn)

    // Combine the models with the ParamMaps they were trained from.
    val modelForParamMapString: HashMap[String, M] = 
      HashMap[String, M](paramMapsToTrainOn.map({_.toString}).zip(models):_*)


    // Define a helper to walk the chain of subsumers to find the ParamMap which has no subsumers.
    @tailrec
    def getSubsumer(pm: ParamMap): ParamMap = 
      subsumedBy.get(pm.toString) match {
        case Some(subsumingPm) => if (subsumingPm == pm) pm else getSubsumer(subsumingPm)
        case None => pm
      }

    // For each original ParamMap in paramMaps, figure out which model it has been given and return
    // the results.
    paramMaps.map{pm => modelForParamMapString.get(getSubsumer(pm).toString).get}
  }
}
