var db = require('monk')('localhost/wahootest');
var models = db.get('models');

/**
 * This utility class handles accessing and modifying
 * the MongoDB collection that stores the trained models.
 */
var Utils = function() {
  var that = Object.create(Utils.prototype);

  /**
   * Finds all the models in the database.
   * @param callback - the callback function to which the array
   *  holding all models is passed.
   */
  that.findAll = function(callback) {
    models.find({}, function(err, allModels) {
      callback(err, allModels);
    });
  };

  Object.freeze(that);
  return that;
};

module.exports = Utils();
