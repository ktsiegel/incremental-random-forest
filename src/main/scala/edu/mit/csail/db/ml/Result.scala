package edu.mit.csail.db.ml

// Essentially just a hashmap of attribute -> numerical value
// Should contain interesting metrics from the evaluator
class Result(myAccuracy: Integer) {

  // example attribute
  var accuracy: Integer = myAccuracy

}
