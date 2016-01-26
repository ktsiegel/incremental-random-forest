/**
 * Container of app pages (and navigation bar).
 */
import React from 'react';
import Router from 'react-router';

import PageNav from '../components/PageNav.js';

class App extends React.Component {
  render() {
    return (
			<div className="container-fluid">
				<PageNav />
        <div className="page-container">
          <Router.RouteHandler/>
        </div>
			</div>
    );
  }
}

export default App;
