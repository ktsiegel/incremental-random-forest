package org.apache.spark.ml

import org.apache.spark.sql.DataFrame

/**
 * An extension to ModelDb with test-specific code. This is necessary so that our unit tests can 
 * ensure that the database behaves the way we expect. For example, the TestDb contains a field 
 * which indicates when it has read a cached model (i.e. a model stored in the database).
 */
class TestDb(databaseName: String, modelCollectionName: String) 
extends ModelDb(databaseName, modelCollectionName) {
  /**
   * Whether the TestDb has fetched a model from the database.
   */
  var fromCache: Boolean = false

  /**
   * Like the super's get or else, but we update the fromCache instance variable.
   */
  override def getOrElse[M <: Model[M]](spec: ModelSpec[M], dataset: DataFrame)(orElse: ()=> M): M = {
    if (get(spec, dataset) != null) fromCache = true
    else fromCache = false
    super.getOrElse(spec, dataset)(orElse)
  }
}
