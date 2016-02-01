/**
 * The navigation bar which is always displayed at the top of the app.
 * This is just a Twitter Bootstrap navigation bar.
 */

import React from 'react';
import Router from 'react-router';

class PageNav extends React.Component {
  render() {
    return (
      <nav className="navbar navbar-default navbar-fixed-top">
        <div className="container">
          <div className="navbar-header">
            <button type="button" className="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
              <span className="sr-only">Toggle navigation</span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
              <span className="icon-bar"></span>
            </button>
            <a className="navbar-brand" href="/">WahooML</a>
          </div>
          <div id="navbar" className="navbar-collapse collapse">
            <ul className="nav navbar-nav navbar-right">
              <li><Router.Link to="models">Models</Router.Link></li>
              <li><Router.Link to="runs">Runs</Router.Link></li>
            </ul>
          </div>
        </div>
      </nav>
    );
  }
}

export default PageNav
