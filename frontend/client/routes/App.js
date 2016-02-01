/**
 * This component serves as a container that will contain pages of the app (each
 * page is a component that may contain other components within it).
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
