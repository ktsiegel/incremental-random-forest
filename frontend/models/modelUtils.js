'use strict';

/**
 * This utility class handles accessing and modifying
 * the MongoDB collection that stores the trained models.
 */

const constants = require('../config/constants');
const db = require('monk')(constants.MONGO_URL);
const models = db.get('models');

class Utils {
  /**
   * Finds all the models in the database.
   * @param callback - Executed as callback(err, models),
   *   where models is the list of MongoDB documents.
   */
  findAll(callback) {
    models.find({}, callback)
  }
}

// Export a singleton.
module.exports = new Utils();
