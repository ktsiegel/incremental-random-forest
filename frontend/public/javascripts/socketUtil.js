// Socket code for logging page.
var socket = io.connect('http://localhost:3000');
socket.on('connection', function (data) {
  console.log("Socket connected.");
  socket.emit('ack', { status: 'Success!' });
});
socket.on('message', function(data) {
  // Append log message to DOM
  var message = $('<div class="message">' + data.content+ '</div>');
  $('#run-container').append(message);
});
