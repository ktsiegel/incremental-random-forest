/**
 * Table displaying the models that have been trained. It will be displayed on
 * the "Models" page.
 */

import React from 'react';
import ModelTableRow from './ModelTableRow';

class ModelTable extends React.Component {
  constructor(props) {
    super(props)
  }
  render() {
    // Render a row for each model.
    const modelTableRows = this.props.models.map((model) => {
      return (
        <ModelTableRow model={model}></ModelTableRow>
      )
    });

    // Display the table header and each of the rows.
    return (
      <table className="table table-striped row">
        <thead>
          <tr className="row">
            <th>UID</th>
            <th>Weights</th>
            <th>Intercept</th>
            <th>ModelSpec</th>
            <th>DataFrame ID</th>
          </tr>
        </thead>
        <tbody>
          {modelTableRows}
        </tbody>
      </table>
    );
  }
}

export default ModelTable
