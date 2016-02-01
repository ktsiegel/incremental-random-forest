'use strict';

/**
 * Combine each of the API endpoints into a single Router.
 */

const express = require('express');
const router = express.Router();

router.use('/logs', require('./log-messages'));
router.use('/models', require('./models'));

module.exports = router;
