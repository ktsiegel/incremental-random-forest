/**
 * Configure react-router.
 */
 
import React from 'react';
import Router from 'react-router';

import Models from './Models';
import Runs from './Runs';
import Welcome from './Welcome';

function configure(App) {
  return (
    <Router.Route name="app" path="/" handler={App}>
  		<Router.Route name="models" path="/models" handler={Models}/>
  		<Router.Route name="runs" path="/runs" handler={Runs}/>
  		<Router.DefaultRoute handler={Welcome}/>
  	</Router.Route>
  );
}

export default configure;
