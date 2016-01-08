'use strict';

/**
 * Exports the socket.io library for use in routes.
 * Sets up the basic handshake upon socket connection between
 * frontend and server.
 */
const io = require('socket.io')();

io.on('connection', (socket) => {
  console.log('connection!');
  socket.emit('connection', { message: 'connected!' });
  socket.on('ack', (data) => console.log(data))
});

module.exports = io;

