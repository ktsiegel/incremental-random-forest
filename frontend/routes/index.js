'use strict';

const express = require('express');
const router = express.Router();
const io = require('../io');
const runLogs = require('../models/runLogs')

const modelUtils = require('../models/modelUtils');

// Welcome page
router.get('/',  (req, res, next) => res.render('index', {}));

// Receive log message from Spark WahooConnector
router.post('/', (req, res, next) => {
  runLogs.push(req.body);
  io.emit('message', req.body);
  res.send('received');
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
  res.render('runs', {'runLogs': runLogs});
});

module.exports = router;
