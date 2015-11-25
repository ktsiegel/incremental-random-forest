var express = require('express');
var router = express.Router();
var io = require('../io');
var runLogs = require("../models/runLogs")

var modelUtils = require('../models/modelUtils');

// Welcome page
router.get('/', function(req, res, next) {
  res.render('index', {});
});

// Receive log message from Spark WahooConnector
router.post('/', function(req, res, next) {
  runLogs.push(req.body);
  io.emit('message', req.body);
  res.send("received!");
});

// See all models in the DB
router.get('/models', function(req, res, next) {
  modelUtils.findAll(function(err, allModels) {
    if (err) { res.status(500).send(); }
    else {
      res.render('models', { models: allModels });
    }
  });
});

// See the logs for all job runs
router.get('/runs', function(req, res, next) {
  res.render('runs', {"runLogs": runLogs});
});

module.exports = router;
