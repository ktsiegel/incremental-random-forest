'use strict';

const express = require('express');
const router = express.Router();
const io = require('../io');
const LogMessageModel = require('../models/LogMessage')

const modelUtils = require('../models/modelUtils');

// Welcome page
router.get('/',  (req, res, next) => res.render('index', {}));

// Receive log message from Spark WahooConnector
router.post('/', (req, res, next) => {
  LogMessageModel.logMessage(req.body.message, (err, data) => {
    if (err) {
      res.status(500).send(err.toString());
    } else {
      io.emit("message", data);
      res.send("received");
    }
  });
});


// See all models in the DB
router.get('/models', (req, res, next) => {
  modelUtils.findAll((err, allModels) => {
    if (err) { 
      res.status(500).send(); 
    } else {
      res.render('models', { models: allModels });
    }
  });
});

// See the logs for all job runs
router.get('/runs', (req, res, next) => {
  LogMessageModel.getAllMessages((err, messages) => {
    if (err) {
      res.status(500).send(err.toString());
    } else {
      res.render('runs', {'runLogs': messages});
    }
  });
});

module.exports = router;
