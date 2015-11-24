var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', {});
});

router.post('/', function(req, res, next) {
  console.log("received request with body: " + req.body);
  res.send("received!");
});

router.get('/models', function(req, res, next) {
  res.render('models', {});
});

router.get('/runs', function(req, res, next) {
  res.render('runs', {});
});

module.exports = router;
