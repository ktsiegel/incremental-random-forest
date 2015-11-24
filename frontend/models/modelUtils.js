var db = require('monk')('localhost/wahootest');
var models = db.get('models');

var Utils = function() {
  var that = Object.create(Utils.prototype);

  that.findAll = function(callback) {
    models.find({}, function(err, allModels) {
      callback(err, allModels);
    });
  };

  Object.freeze(that);
  return that;
};

module.exports = Utils();
