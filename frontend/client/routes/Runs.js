/**
 * The Runs page which displays the message logs.
 */

import React from 'react';
import Api from '../util/Api';

const api = new Api();

class Runs extends React.Component {
  constructor(props) {
    super(props);

    // The Wahoo workers will log messages, and we display them on this page.
    this.state = {
      'logs': []
    }

    // Connect to the server via websockets and add messages to the log as they
    // come in.
    const that = this;
    api.addSocketListener((data) => {
      const oldLogs = this.state.logs.concat(data);
      that.setState({
        'logs': oldLogs
      });
    });
  }
  componentDidMount() {
    // Set up the websocket connection and fetch the latest logs over HTTP.
    api.connectToSocket();
    api.getLogs((err, logs) => {
      if (!err) {
        this.setState({
          logs
        });
      }
    })
  }
  render() {
    // Create a paragraph for each message (and its timestamp).
    const messageNodes = this.state.logs.map((log) => {
      return <p>{log.content} @ {log.posted}</p>
    });

    // This page just displays a header and the list of messages.
    return (
      <div>
        <h3>Runs</h3>
        {messageNodes}
      </div>
    );
  }
}

export default Runs
