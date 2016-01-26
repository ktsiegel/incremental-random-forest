/**
 * Represents a row in the table of models.
 */

import React from 'react';

class ModelTableRow extends React.Component {
  constructor(props) {
    super(props)
  }
  render() {
    const model = this.props.model;

    // Display the model weights.
    const weightNodes = model.weights.map((wt) => {
      return (
        <p>{wt}</p>
      );
    });

    // The model spec is a JSON object, and we want to display the key-value
    // pairs.
    const keyValNodes = Object.keys(model.modelspec).filter((key) => {
      return model.modelspec.hasOwnProperty(key);
    }).map((key) => {
      const val = model.modelspec[key];
      return (
        <p>{key}: {val}</p>
      )
    });

    // Truncate the data frame id because it's really long.
    const dataFrameNode = model.dataframe.slice(0, 10);

    // Round the intercept.
    const intercept = model.intercept.toFixed(2);

    return (
      <tr className="row">
        <td className="col-sm-2">{model.uid}</td>
        <td className="col-sm-3">
          {weightNodes}
        </td>
        <td className="col-sm-2">{intercept}</td>
        <td className="col-sm-3">
          {keyValNodes}
        </td>
        <td className="col-sm-2">{dataFrameNode}...</td>
      </tr>
    );
  }
}

export default ModelTableRow
