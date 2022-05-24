const credentials = require("./credentials.json");
const config = require("./config.json")
const Sync = require("./src/DataWarehouse.js");




const GatherClient = require("gather-node-client");
new GatherClient(credentials, config, (client) => {

	(new Sync(config, client)).syncFilesystem().then((sync)=>{

		return sync.syncCategories().then(()=>{
			process.exit(0);
		})

	})

	

});