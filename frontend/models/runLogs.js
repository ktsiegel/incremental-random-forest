'use strict';
/**
 * In-memory store of logs sent by Spark. Note that this
 * is a temporary solution until we develop a schema for the logs
 * and finalize which logs WahooML will send to the frontend.
 * Also note that all logs are lost upon a server stop or crash.
 */
const runLogs = [];

module.exports = runLogs;
