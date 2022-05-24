const EventEmitter = require("events");
const chokidar = require('chokidar');
const md5File = require('md5-file');
const fs = require("fs");

const chalk = require("chalk");

module.exports = class DataWarehouse extends EventEmitter {



	constructor(config, client) {


		super();

		var counter = 0;

		this._tree = {
			category: config.rootCategory,
			children: {}

		}

		this._files = [];

		this._client=client;
		this._config=config;

		console.log("Watching Directory: "+chalk.yellow(config.path));

		chokidar.watch(config.path).on('all', (event, path) => {

			if (path.toLowerCase().indexOf('.shp') === path.length - 4) {
				//console.log(path.split('/').pop().split('.').slice(0, -1).join('.'));
				counter++;

				var file = path.split(config.path).pop();
				//console.log(file);

				this._addPath(file);


				md5File(path).then((hash) => {


					let stats=fs.statSync(path);
					this._files.push({
						file: file,
						md5: hash,
						stats:{
							size:stats.size,
							ino:stats.ino,
							//_info:Object.keys(stats)
						}
					});
					this._checkIdle();
				});

			}
			this._checkIdle();

		});

	}

	syncFilesystem() {


		console.log('list');

		return new Promise((resolve)=>{

			Promise.all([this._client.listProjects(), this._client.listArchivedProjects(), this.getCategories(), this.getFiles()]).then((lists) => {




				const projectsData = lists[0].filter((project) => {
					return project.metadata && project.metadata.file;
				});
				const projects=projectsData.map((project) => {
					return project.metadata.file;
				});
				const archive = lists[1].filter((project) => {
					return project.metadata && project.metadata.file;
				}).map((project) => {
					return project.metadata.file;
				});
				const warehouse = lists[2];
				const files = lists[3];


				console.log();
				console.log('Found '+files.length+' files - Processing');
				console.log();


				this._markDuplicates(files);



				var found = 0
				var missing = 0;
				var newFiles = [];
				var updates=0;


				files.sort(function(fileA, fileB){
					let c=fileA.md5.localeCompare(fileB.md5);
					if(c==0){
						if(fileA.duplicates){
							return -1;
						}
						if(fileB.duplicates){
							return 1;
						}
					}
					return c;
				})


				files.forEach((file) => {

					if(!(file.duplicates||file.alias)){
						return;
					}

					if(file.duplicates){
						console.log("");
					}
					console.log((file.alias?chalk.strikethrough.red("D "+file.md5):(file.duplicates?chalk.cyan(file.md5):file.md5))+" "+file.stats.ino+" "+(file.file.split('/').pop())+(file.alias||file.duplicates?"\t\t\t"+file.file:""));
				});




				let mainFiles=files.filter((file) => {
					return !file.alias;
				});


				let processedProjectIds=[];


				console.log("Processing "+mainFiles.length+" main files");

				mainFiles.forEach((file) => {

					

					var activeProject = this._getProjectMatch(file, projectsData);
					if (activeProject) {
						found++;

						updates+=this._syncProjectFile(activeProject, file);

						processedProjectIds.push(parseInt(activeProject.id));
						var category=activeProject.file


					} else {
						missing++;
						newFiles.push(file)
					}

				});


				console.log("Finished Processed (Matched "+processedProjectIds.length+" File/Projects)");
				console.log();






				let unprocessedProjectsData=projectsData.filter((project)=>{
					return processedProjectIds.indexOf(parseInt(project.id))==-1;
				});


				console.log("Processing "+unprocessedProjectsData.length+" remaining unmatched/duplicate projects");

				unprocessedProjectsData.forEach((project) => {

					var activeFile = this._getFileMatch(project, files);
					if (activeFile) {
						if(activeFile.alias){
							console.log(chalk.red(activeFile.md5)+" Matched alias file/project: "+project.id);
							//console.log(JSON.stringify(activeFile));

							this._syncProjectFile(project, activeFile);
						}


						
					} else {
						console.log('Failed to find file for project: '+project.id+': '+JSON.stringify(project.metadata));
						this._archiveProject(project.id);
					}

				});







				let aliasFiles=files.filter((file)=>{
					return (!!file.alias)&&!file.project;
				});

				if(aliasFiles.length>0){
					console.log("Process "+aliasFiles.length+" remaining files");
				}
				
				aliasFiles.forEach((file) => {
					newFiles.push(file);
				});


				





				console.log('found ' + found + " existing projects, and " + missing + " new files. Required "+updates+" project updates");




				if(newFiles.length>0){
					console.log('Creating '+newFiles.length+' new projects');
				}
				newFiles.forEach((file)=>{

					/**
					 * could check archive first
					 */
					this._createNewFileProject(file);
				});


				console.log('Finished syncing files');


				resolve(this);

			});


		});


	}

	_syncProjectFile(project, file){

		let updates=0;

		if(file.file!==project.metadata.file.file){

			console.log('file name changed');

			console.log('Project('+project.id+') File needs update: '+project.metadata.file.file+" "+project.metadata.file.md5);
			console.log("\t"+'===>: '+JSON.stringify(file));

			updates++;

			this._updateProjectMetadata(project.id, { 
				iam: 'gatherbot',
					file: { 
						file: file.file,
					md5: project.metadata.file.md5
				}});
		}


		if(file.md5!==project.metadata.file.md5){
			console.log('file content changed');
		}


		var category=project.file

		return updates;

	}


	_updateProjectMetadata(project, metadata){
		console.log("update project metadata");
		
		return this._client.updateProjectMetadata({
			id:parseInt(project),
			metadata:metadata
		}).then((data)=>{
			console.log(data.id);

			
			console.log('Expected: '+JSON.stringify(metadata));
			console.log('Actual: '+JSON.stringify(data.metadata));
		});


	}


	_markDuplicates(files){

		/**
		 * Marks any duplicate files with alias value = the ino of the first match
		 * and adds duplicate file ino to a duplcates list on the first match 
		 */


		var md5s=files.map((f)=>{ 
			return f.md5; 
		});

		var duplicates=md5s.filter((md5, i)=>{
			var j=md5s.indexOf(md5)
			if(j!==i){

				let dup=files[i];
				let main=files[j]

				console.log(dup.md5+" "+dup.stats.ino+" "+(dup.file.split('/').pop())+" is a duplicate of "+main.stats.ino+" "+(main.file.split('/').pop()));
					

				if(!files[j].duplicates){
					files[j].duplicates=[];
				}
				files[j].duplicates.push(files[i].stats.ino)
				files[i].alias=files[j].stats.ino;

				return true;
				
			}
			return false;
		});

		console.log("Found "+duplicates.length+" duplicate files");

	}


	_createNewFileProject(file){

		

		console.log("create project");
		console.log(file);
		return this._client.createProject({
			metadata:{
				iam:this._config.iam,
				file:file
			},
			attributes:{
				proposalAttributes:{
					title:file.file.split('/').pop().split('.').slice(0,-1).join('.'),
					isDataset:true
				}
			}
		}).then((data)=>{
			console.log(data);
		});

			


	}
	_archiveProject(id){


		id=parseInt(id);

		this._client.archiveProject(id).then((data)=>{


			console.log('archive project: '+id);
			console.log(JSON.stringify(data));

		});

	}


	syncCategories() {


		console.log('Syncing Categories');


		return new Promise((resolve, reject) => {

		this._client.listCategories().then((list) => {
			this.getCategories().then((fileCats)=>{

				var serverCats=list.filter((c)=>{
					return c.metadata.iam&&c.metadata.iam==='gatherbot';
				});


				var matchedCategories=[]; 
				var created=0;

				fileCats.forEach((folderCat)=>{
					var matches=serverCats.filter((c)=>{
						return c.category==folderCat.type&&c.name==folderCat.name;
					});

					if(matches.length==0){
						console.log("Missing category: "+folderCat.type+' => '+folderCat.name.substring(folderCat.type.length));
						//{"type":"data warehouse/stsailesdatafolder/archeological/teal","name":"data warehouse/StsailesDataFolder/Archeological/Teal/MC205","metadata":{"iam":"gatherbot","selectable":false,"editable":false},"shortName":"MC205","path":"data warehouse/StsailesDataFolder/Archeological/Teal/"}
						this._client.createCategory({
							name:folderCat.name,
							category:folderCat.type,
							description:'',
							shortName:folderCat.shortName,
							metadata:folderCat.metadata,
							color:null
						}).then((cat)=>{
							console.log(cat);
						})

					}

					if(matches.length>1){
						throw 'Unexpected multiple results: '+JSON.stringify(matches);
					}

					if(matches.length=='1'){
						//console.log('found category: '+folderCat.name+' '+folderCat.path);
						matchedCategories.push(matches[0].id)
					}

				});

				let unmatchedCats=serverCats.filter((cat)=>{
					return matchedCategories.indexOf(cat.id)==-1;
				});

				console.log('Found '+fileCats.length+' categories, matched '+matchedCategories.length+'/'+serverCats.length+' server categories');
				console.log('Removing '+unmatchedCats.length+' deprecated categories');
				unmatchedCats.forEach((cat)=>{
					this._client.removeCategory(parseInt(cat.id));
				});

				console.log('Done syncing categories: '+JSON.stringify(matchedCategories));
				
				resolve(matchedCategories);

			}).catch(reject);
		}).catch(reject);


		});

	}

	_getFileMatch(project, files) {


		/**
		 * select file given project
		 */


		let results=files.filter((file)=>{
			return file.project&&file.project===parseInt(project.id);
		});

		if(results.length>0){
			return results[0];
		}



		let resultsMd5=files.filter((file)=>{
			return file.md5==project.metadata.file.md5;
		})

		results=resultsMd5.filter((file)=>{
			return !file.project;
		});





		if(results.length==0&&resultsMd5.length>0){
			console.log('Failed to match file but has md5 matches');
			console.log(JSON.stringify(resultsMd5, null, '   '));
			console.log('');
		}

		if(results.length>0){


			if(results.length>1){
				let exactPathResults=results.filter((file)=>{
					return file.file==project.metadata.file.file;
				});
				if(exactPathResults.length>0){
					exactPathResults[0].project=parseInt(project.id);
					exactPathResults[0].match='alias exactMatch';
					return exactPathResults[0];
				}


				// let exactNameMatches=results.filter((file)=>{
				// 	return file.file==project.metadata.file.file;
				// });
				// if(exactNameMatches.length>0){
				// 	exactNameMatches[0].project=parseInt(project.id);
				// 	exactNameMatches[0].match='alias exactMatch';
				// 	return exactNameMatches[0];
				// }

			}



			results[0].project=parseInt(project.id);
			results[0].match='unmatched md5 match';

			return results[0];
		}

		return null;

	}

	_getProjectMatch(file, list) {

		/**
		 * select project given file
		 */

		let exactPathResults=list.filter((p)=>{
			return file.file == p.metadata.file.file;
		});

		if(exactPathResults.length>=1){

			file.project=parseInt(exactPathResults[0].id);
			file.match='exactMatch'

			return exactPathResults[0];
		}



		let md5Results=list.filter((p)=>{
			return file.md5 == p.metadata.file.md5;
		});

		if(md5Results.length>=1){

			file.project=parseInt(md5Results[0].id);
			file.match='md5Match'

			return md5Results[0];
		}

		return null;

	}


	getFiles() {

		return new Promise((resolve, reject) => {

			console.log('wait for idle: scanning folders');

			this._onIdle(() => {
				resolve(this._files.slice(0));
			});

		})


	}

	getCategories() {

		return new Promise((resolve, reject) => {

			console.log('wait for idle: scanning folder categories');

			this._onIdle(() => {
				resolve(this._flattenTreeChildrenBFS(this._tree));
			});

		})

	}

	_onIdle(fn) {

		if (this._isIdle) {
			fn();
			return;
		}

		this.once('idle', fn);

	}


	_addPath(path) {



		var node = this._tree;

		path.split('/').slice(0, -1).forEach((part) => {


			if (typeof node.children[part] == "undefined") {
				node.children[part] = {
					category: part,
					children: {}
				}
			}

			node = node.children[part];

		});


	}

	_checkIdle() {

		if (this._idleTimer) {
			clearTimeout(this._idleTimer);
		}

		this._idleTimer = setTimeout(() => {

			delete this._idleTimer;

			//console.log(this._toSql(this._flattenTreeChildrenBFS(this._tree)));

			this.emit('idle');
			this._isIdle = true;

			// console.log(JSON.stringify(this._tree , null, '   '));
			//console.log(JSON.stringify(this._flattenTreeChildrenBFS(this._tree) , null, '   '))
			

		}, 1000);
	}


	_flattenTreeChildrenBFS(tree, parent) {

		if ((!parent) || typeof parent == "undefined") {
			parent = tree.category.toLowerCase();
		}

		var list = [];



		//BFS

		var nodes = [{
			category: '',
			name: tree.category,
			children: tree.children,
			path:''
		}];
		while (nodes.length > 0) {

			var n = nodes.shift();
			list.push({
				type: n.category.toLowerCase(),
				name: n.path+n.name,
				metadata: {"iam":"gatherbot", "selectable":false, "editable":false},
				shortName: n.name,
				path:n.path
			});

			Object.keys(n.children).forEach((child) => {

				nodes.push({
					category: n.path+n.name,
					name: child,
					children: n.children[child].children,
					path:n.path+n.name+'/'
				});

			});


		}


		return list.slice(1);


	}

	// _flattenTreeChildrenDFS(tree, parent) {

	// 	if ((!parent) || typeof parent == "undefined") {
	// 		parent = tree.category.toLowerCase();
	// 	}



	// 	var list = [];

	// 	//DFS

	// 	Object.keys(tree.children).forEach((child) => {

	// 		list.push({

	// 			type: parent,
	// 			name: child,
	// 			metadata: {},
	// 			shortName: child

	// 		});

	// 		list = list.concat(this._flattenTreeCategories(tree.children[child], child));

	// 	});


	// 	return list;

	// }


	_toSql(flatList) {

		var keys=['type', 'name', 'metadata', 'shortName'];

		return `INSERT INTO $tablename (` + keys.join(', ') + `) VALUES (` + flatList.map((n) => {
			n['metadata'] = JSON.stringify(n['metadata']);
			return '"' + keys.map((k) => {
				return n[k]
			}).join('", "') + '"';
		}).join('), \n (') + ');';

	}

}