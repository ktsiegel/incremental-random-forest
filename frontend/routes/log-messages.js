'use strict';

/**
 * Endpoint for adding and retrieving log messages.
 */

const express = require('express');
const router = express.Router();
const LogMessageModel = require('../models/LogMessage')
const SocketIO = require('../config/socketio');

// Add a message to the log.
router.post('/', (req, res) => {
  LogMessageModel.logMessage(req.body.message, (err, data) => {
    if (err) {
      res.fail(500, err);
    } else {
      // We send the message to anybody who will listen.
      SocketIO.broadcast('message', data);
      res.success('recieved', data);
    }
  });
});

// Get all the logged messages.
router.get('/', (req, res) => {
  LogMessageModel.getAllMessages((err, messages) => {
    if (err) {
      res.fail(500, err.toString());
    } else {
      res.success('log messages', messages);
    }
  });
});

module.exports = router;
