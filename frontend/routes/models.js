'use strict';

/**
 * Endpoint for retrieving the machine learning models that have been trained.
 */

const express = require('express');
const router = express.Router();
const modelUtils = require('../models/modelUtils');

// Get all the models in the database.
router.get('/', (req, res, next) => {
  modelUtils.findAll((err, allModels) => {
    if (err) {
      res.fail(500, err);
    } else {
      res.success('models', allModels);
    }
  });
});

module.exports = router;
