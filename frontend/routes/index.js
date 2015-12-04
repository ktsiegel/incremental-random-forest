var express = require('express');
var router = express.Router();
var io = require('../io');
var runLogs = require("../models/runLogs")

var modelUtils = require('../models/modelUtils');

// Welcome Page
// TODO: turn index.html into a 'Session Log-In' page after multi-user development starts
router.get('/', function(req, res, next) {
  //res.render('index', {});
  modelUtils.findAll(function(err, allModels) {
    if (err) { res.status(500).send(); }
    else {
      res.render('models', { models: allModels });
    }
  });
});

// Receive log message from Spark WahooConnector
router.post('/', function(req, res, next) {
  runLogs.push(req.body);
  io.emit('message', req.body);
  res.send("received!");
});

// Models Page - (see all models in the DB)
// TODO: re-instate this route after multi-user development starts
/*router.get('/models', function(req, res, next) {
  modelUtils.findAll(function(err, allModels) {
    if (err) { res.status(500).send(); }
    else {
      res.render('models', { models: allModels });
    }
  });
});*/

// Runs Page - (see the logs for all job runs)
router.get('/runs', function(req, res, next) {
  res.render('runs', {"runLogs": runLogs});
});

module.exports = router;
