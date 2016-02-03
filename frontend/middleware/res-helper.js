'use strict';

/**
 * This file defines middleware that adds convenience functions to the res (i.e.
 * HTTP response) object used by Express.
 */

module.exports = function(req, res, next) {

  /**
   * Respond with a JSON object indicating success and use a 200 (OK) status
   * code.
   */
  res.success = function(message, content) {
    res.status(200).json({
      'success': true,
      message,
      content
    });
  };

  /**
   * Respond with a JSON object indicating failure and the given status code.
   */
  res.fail = function(code, error) {
    res.status(code).json({
      'success': false,
      'error': error.toString()
    });
  };
  next();
};
