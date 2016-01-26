'use strict';

/**
 * Handles configuring and handling connections to Socket.io on the server side.
 */

const socketio = require("socket.io");

class SocketIOConfig {
  constructor() {
    // List of client connections.
    // TODO: Close these connections when clients disconnect.
    this.sockets = [];

    // Whether the Socket.io has already been set up.
    this.hasConfigured = false;
  }

  // Set up Socket.io on the server side using the given server.
  setup(server) {
    if (this.hasConfigured) {
      throw new Error("Tried to config socket io twice");
    }

    this.io = socketio(server);
    const that = this;

    this.io.on('connection', (socket) => {
      that.sockets.push(socket);
      console.log('connection!');

      // Handshake indicating successful connection.
      // TODO: Only add the connection to the list of sockets if the Handshake
      // is completed.
      socket.emit('connection', {
        'message': 'connected'
      });
      socket.on('ack', (data) => {
        console.log(data);
      });
    });

    this.hasConfigured = true;
  }

  // Send a message to everybody who is connected.
  broadcast(messageName, data) {
    this.sockets.forEach((socket) => {
      socket.emit(messageName, data);
    });
  }
}

const config = new SocketIOConfig();

module.exports = config;
