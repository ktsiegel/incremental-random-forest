'use strict';

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

/**
 * Create a log message.
 * @param {String} content - The text content of the log message.
 * @param {Function} callback - callback(err, document).
 */
LogMessageSchema.statics.logMessage = function(content, callback) {
  this.create({content}, callback);
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
      callback(null, messages.map((msg) => {
        return {
          'content': msg.content,
          'posted': msg.posted
        }
      }));
    }
  });
};

module.exports = mongoose.model('LogMessage', LogMessageSchema);
