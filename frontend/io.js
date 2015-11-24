var io = require('socket.io')();

io.on('connection', function (socket) {
  console.log("connection!");
  socket.emit('connection', { message: 'connected!' });
  socket.on('ack', function (data) {
    console.log(data);
  });
});

module.exports = io;

