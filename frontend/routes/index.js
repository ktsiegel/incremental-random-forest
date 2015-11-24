var express = require('express');
var router = express.Router();
var io = require('../io');
var runLogs = require("../models/runLogs")

var modelUtils = require('../models/modelUtils');

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', {});
});

router.post('/', function(req, res, next) {
  console.log("received request with body: " + req.body);
  runLogs.push(req.body);
  io.emit('message', req.body);
  res.send("received!");
});

router.get('/models', function(req, res, next) {
  modelUtils.findAll(function(err, allModels) {
    if (err) { res.status(500).send(); }
    else {
      res.render('models', { models: allModels });
    }
  });
});

router.get('/runs', function(req, res, next) {
  res.render('runs', {"runLogs": runLogs});
});

module.exports = router;
