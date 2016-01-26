/**
 * Models page which displays the models that have been trained.
 */
import React from 'react';
import Api from '../util/Api';
import ModelTable from '../components/ModelTable';

const api = new Api();

class Models extends React.Component {
	constructor(props) {
		super(props);
		this.state = {
			'models': []
		};
	}
	componentDidMount() {
		// When the component is loaded, fetch the models and populate the tables.
		api.getModels((err, models) => {
			if (!err) {
				this.setState({
					models
				});
			}
		});
	}
	render() {
		const models = this.state.models;
		return (
			<div className="models">
				<h3>Models</h3>
				<ModelTable models={models}></ModelTable>
			</div>
		);
	}
}

export default Models
