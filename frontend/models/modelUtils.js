'use strict';

const db = require('monk')('localhost/wahootest');
const models = db.get('models');

/**
 * This utility class handles accessing and modifying
 * the MongoDB collection that stores the trained models.
 */
class Utils {
  /**
   * Finds all the models in the database.
   * @param callback - Executed as callback(err, models), where models is the list of MongoDB
   * documents.
   */
  findAll(callback) {
    models.find({}, callback)
  }
}

// Export a singleton.
module.exports = new Utils();
