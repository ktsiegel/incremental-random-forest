/**
 * Box welcoming users to Wahoo.
 */
 
import React from 'react';
import Router from 'react-router';

class WelcomeBox extends React.Component {
  render() {
    return (
      <div className="jumbotron">
        <h2 id="welcome-smaller">Welcome to</h2>
        <h1 id="welcome">WahooML</h1>
      </div>
    );
  }
}

export default WelcomeBox
