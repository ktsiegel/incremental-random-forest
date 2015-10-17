package edu.mit.csail.db.ml

/**
 * This class serves as the interface to the modelDB that stores all the information
 * about currently and previously trained models and results of their evaluation
 *
 * This will likely be implemented as a database underneath and this is just a wrapper
 * around it.
 *
 * The tables are:
 * ModelSpecs
 * ModelEval
 * TrainingRun
 * Model??
 * Created by mvartak on 10/14/15.
 */
class ModelDB {
  def query(query: String): Unit = {

  }

  def update(query: String): Unit = {

  }
}
