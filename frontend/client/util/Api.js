
/**
 * Convenience functions for interacting with the API.
 */

// Helper function to avoid code repetition.
const wrapCallback = (callback) => {
  return (data) => {
    if (data.success) {
      callback(null, data.content);
    } else {
      callback(data.error);
    }
  }
};

class Api {
  constructor() {
    // Whether we've connected to the server via websockets.
    this.socketConnected = false;

    // The functions to execute when we get a message from the server via
    // websockets.
    // TODO: This should be at a finer granularity, because some modules may
    // only care about specific kinds of events.
    this.socketListeners = [];
  }

  /**
   * Connect to the server's websocket port. If we've already connected, then
   * this function does nothing.
   */
  connectToSocket() {
    // Don't connect if we already have.
    if (this.socketConnected) {
      return;
    }

    // Connect and perform handshake.
    const socket = io();
    socket.on('connection', (data) => {
      console.log('Connected to Wahoo Web App websocket port');
      socket.emit('ack', {
        'status': 'Success!'
      });
    });

    // When we get a message from this socket, notify all listeners.
    const socketListeners = this.socketListeners;
    socket.on('message', (data) => {
      socketListeners.forEach((listener) => {
        listener(data);
      });
    });
    this.socketConnected = true;
  }

  /**
   * Add a listener which is triggered when a new message comes in on the
   * websocket. The argument, listener, will be executed as listener(message).
   */
  addSocketListener(listener) {
    this.socketListeners.push(listener);
  }

  /**
   * Fetch the models that have been trained so far. The callback is executed
   * as callback(err, data).
   */
  getModels(callback) {
    $.get('/api/models', wrapCallback(callback));
  }

  /**
   * Fetch the log of messages. The callback is executed as callback(err, data).
   */
  getLogs(callback) {
    $.get('/api/logs', wrapCallback(callback));
  }
}

export default Api
