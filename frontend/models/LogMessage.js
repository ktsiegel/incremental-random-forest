'use strict';

/**
  * The Mongoose model for Log messages.
  */

const mongoose = require("mongoose");

const LogMessageSchema = mongoose.Schema({
  'posted': {
    'type': Date,
    'default': Date.now,
    'required': true,
  },
  'content': {
    'type': String,
    'required': true,
  }
});

const cleanMessage = function(msg) {
  return {
    'content': msg.content,
    'posted': msg.posted
  };
}

/**
 * Create a log message.
 * @param {String} content - The text content of the log message.
 * @param {Function} callback - callback(err, document).
 */
LogMessageSchema.statics.logMessage = function(content, callback) {
  this.create({content}, (err, message) => {
    if (err) {
      callback(err);
    } else {
      callback(null, cleanMessage(message));
    }
  });
};

/**
 * Fetch all the log messages from the database.
 * @param {Function} callback - callback(err, documents), where 'documents' is an array.
 */
LogMessageSchema.statics.getAllMessages = function(callback) {
  this.find({}, (err, messages) => {
    if (err) {
      callback(err);
    } else {
      callback(null, messages.map(cleanMessage));
    }
  });
};

module.exports = mongoose.model('LogMessage', LogMessageSchema);
