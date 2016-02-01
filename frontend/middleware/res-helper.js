'use strict';

module.exports = function(req, res, next) {
  res.success = function(message, content) {
    res.status(200).json({
      'success': true,
      message,
      content
    });
  };
  res.fail = function(code, error) {
    res.status(code).json({
      'success': false,
      'error': error.toString()
    });
  };
  next();
};
