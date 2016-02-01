/**
 * Main entrypoint for client side code.
 */
import React from 'react';
import Router from 'react-router';

import RouteConfigure from '../routes/RouteConfigure';
import App from '../routes/App';

const routes = RouteConfigure(App);

Router.run(routes, Router.HistoryLocation, function (Handler) {
	React.render(<Handler/>, document.body);
});
