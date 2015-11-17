package org.apache.spark.ml

import org.apache.spark.sql.DataFrame

class TestDb(databaseName: String, modelCollectionName: String) 
extends ModelDb(databaseName, modelCollectionName) {
  var fromCache: Boolean = false
  override def getOrElse[M <: Model[M]](spec: ModelSpec[M], dataset: DataFrame)(orElse: ()=> M): M = {
    if (get(spec, dataset) != null) fromCache = true
    super.getOrElse(spec, dataset)(orElse)
  }
}
