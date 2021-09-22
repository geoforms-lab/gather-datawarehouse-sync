
const config = require("./config.json")
const Sync=require("./src/DataWarehouse.js");


(new Sync(config)).getCategories().then((warehouse) => {

	const credentials = require("./credentials.json");

	if (config.ignoreCertError === true) {
		process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0
	}
	const GatherClient = require("gather-node-client");

	const client = (new GatherClient(credentials, config, () => {

		console.log('list');
		// client.listCategories().then((list) => {
		// 	console.log(list);
		// });
		// 
		client.listProjects().then((list) => {
			console.log(list);
		});

	}));


});