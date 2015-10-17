package edu.mit.csail.db.ml

import org.apache.spark.sql.DataFrame

/**
 * This class represents a training run: a model spec and the data it was trained on
 * We currently don't store the generated model but maybe we should?
 * Created by mvartak on 10/14/15.
 */
class TrainingRun (
  modelSpec: ModelSpec,
  data: DataFrame) {
}
