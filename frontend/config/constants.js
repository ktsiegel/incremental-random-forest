'use strict';

/**
 * The set of constants used in the app.
 */

const MONGO_HOST = 'localhost';
const MONGO_DBNAME = 'wahootest';
const MONGO_URL = 'mongodb://' + MONGO_HOST + '/' + MONGO_DBNAME;

module.exports = {
  MONGO_HOST,
  MONGO_DBNAME,
  MONGO_URL
}
