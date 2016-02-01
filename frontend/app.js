'use strict';

// Modules.
const babelify = require('babelify');
const express = require('express');
const browserify = require('browserify-middleware');
const less = require('less-middleware');
const nunjucks = require('nunjucks');
const path = require('path');
const favicon = require('serve-favicon');
const logger = require('morgan');
const cookieParser = require('cookie-parser');
const bodyParser = require('body-parser');
const mongoose = require("mongoose");


// Our modules.
const constants = require('./config/constants');
const routes = require('./routes/api');
const client_packages = require('./config/client-packages');
const resHelper = require('./middleware/res-helper');
const socketIoConfig = require('./config/socketio');

// Create the HTTP server (and the Websocket server).
const app = express();
var http = require('http').Server(app);
socketIoConfig.setup(http);

// Serve the single view we have.
nunjucks.configure('server/templates/views', {
    express: app
});

// Add a success() and fail() function onto each res object.
app.use(resHelper);

// Log HTTP requests.
app.use(logger('dev'));

// Parse HTTP bodies that are urlencoded or have JSON.
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

// Parse cookies.
app.use(cookieParser());

// Compile LESS files into CSS.
app.use(less('public'));
app.use(express.static('public'));

// Precompile common packages and cache them.
app.get('/js/' + client_packages.common.bundle, browserify(client_packages.common.packages, {
  cache: true,
  precompile: true
}));

// Browserify and babelify (i.e. transpile JSX + ES6) JavaScripts.
app.use('/js', browserify('./client/scripts', {
  external: client_packages.common.packages,
  transform: [babelify.configure({
    plugins: ['object-assign']
  })]
}));

// Set up API endpoints.
app.use('/api', routes);

// Set up endpoint to serve index.html.
app.get('*', function(req, res) {
  res.render('index.html');
});

// Connect to the database.
mongoose.connect(constants.MONGO_URL, (err) => {
  if (err) {
    console.log("Failed to connect to database: " + err.toString());
    process.exit(1);
  }

  // On successful connect, launch the server.
  const server = http.listen(process.env.PORT || 3000, function() {
    console.log('\nServer ready on port %d\n', server.address().port);
  });
});
