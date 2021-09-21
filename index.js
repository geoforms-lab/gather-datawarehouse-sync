

const EventEmitter = require("events");
const chokidar = require('chokidar');
const config=require("./config.json")

class Sync extends EventEmitter{



	constructor(config){

		var counter=0;

		
			this._tree={
				category:config.rootCategory,
				children:{}

			}
		
		console.log(config.path);

		chokidar.watch(config.path).on('all', (event, path) => {
		  
			if(path.toLowerCase().indexOf('.shp')===path.length-4){
				console.log(path);
				counter++;
				
				this._addPath(path.split(config.path).pop());
				this._checkIdle();
			}


		});

	}



	getCategories(fn){


		this._onIdle(()=>{

			

		});


	}

	_onIdle(fn){

		if(this._isIdle){
			fn();
			return;
		}

		this.on('idle', fn);

	}


	_addPath(path){

		


		var node=this._tree;

		path.split('/').slice(0,-1).forEach((part)=>{


			if(typeof node.children[part]=="undefined"){
				node.children[part]={
					category:part,
					children:{}
				}
			}

			node=node.children[part];

		});


	}

	_checkIdle(){

		if(this._idleTimer){
			clearTimeout(this._idleTimer);
		}

		this._idleTimer=setTimeout(()=>{
			
			delete this._idleTimer;

			this.emit('idle');

			// console.log(JSON.stringify(this._tree , null, '   '));
			// console.log(JSON.stringify(this._flattenTreeChildrenBFS(this._tree) , null, '   '))
			// console.log(this._toSql(this._flattenTreeChildrenBFS(this._tree)));

		}, 1000);
	}


	_flattenTreeChildrenBFS(tree, parent){

		if((!parent)||typeof parent=="undefined"){
			parent=tree.category.toLowerCase();
		}

		var list=[];



		//BFS

		var nodes=[{
			category:'',
			name:tree.category,
			children:tree.children
		}];
		while(nodes.length>0){

			var n=nodes.shift();
			list.push({
				type:n.category.toLowerCase(),
				name:n.name,
				metadata:{
				},
				shortName:n.name
			});

			Object.keys(n.children).forEach((child)=>{

				nodes.push({
					category:n.name,
					name:child,
					children:n.children[child].children
				});

			});


		}


		return list.slice(1);


	}

	_flattenTreeChildrenDFS(tree, parent){

		if((!parent)||typeof parent=="undefined"){
			parent=tree.category.toLowerCase();
		}



		var list=[];

		//DFS

		Object.keys(tree.children).forEach((child)=>{

			list.push({

				type:parent,
				name:child,
				metadata:{
				},
				shortName:child
				
			});

			list=list.concat(this._flattenTreeCategories(tree.children[child], child));

		});


		return list;

	}


	_toSql(flatList){

		return `INSERT INTO $tablename (`+Object.keys(flatList[0]).join(', ')+`) VALUES (`+flatList.map((n)=>{
			n['metadata']=JSON.stringify(n['metadata']);
			return '"'+Object.keys(n).map((k)=>{ return n[k] }).join('", "')+'"';
		}).join("),\n (")+`);`;

	}

}

(new Sync(config)).getCategories();





