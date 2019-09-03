const ts=(new Date().toString()).split(' ');
console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [info] Kafka Admin Copyright 2019 Jaroslav Peter Prib");

const debugOff=(()=>false);
function debugOn(m) {
	const ts=(new Date().toString()).split(' ');
	if(!debugCnt--) {
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] Kafka Admin debugging turn off");
		debug=debugOff;
	}
	if(debugCnt<0) {
		debugCnt=100;
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] Kafka Admin debugging next "+debugCnt+" debug points");
	}
	console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] Kafka Admin "+(m instanceof Object?JSON.stringify(m):m));
}
let debug=debugOn,debugCnt=100;

function msgProcess(node,msg,e,data) {
	debug({label:"msgProcess",error:e,data:data});
	if(err) {
		let err=typeof err !=="string"?e:err.message;
		if(err.startWith("Broker not available") || err.startWith("Request timed out")) {
			node.warn("Broker not available, queue message and retry connection");
			node.waiting.push(msg);
       		node.brokerNode.connect(node,"Admin",(err)=>{
       			node.error("connection failed, clearing waiting queue "+node.waiting.length);
       			errorWaiting(node,err);
       		})
			return;
		}
		
		node.error(msg.topic+" "+err);
		msg.error=err.toString();
		node.send([null,msg]);   
		return;
	}
	switch (msg.topic) {
		case 'createTopics':
			data.forEach((c,i,a)=>{
				let t=msg.payload.find((cp)=>cp.topic==c.topic);
				debug({label:"createTopic topic",topic:c,data:t});
				if(c.hasOwnProperty('error')) {
					debug({label:"createTopics topic error",data:{topic:msg.topic,error:c.error,payload:[t]}});
					node.send([null,{topic:msg.topic,error:c.error,payload:[t]}]);
					return;
				}
				debug({label:"createTopics topic ok",data:{topic:msg.topic,payload:[c]}});
				node.send({topic:msg.topic,payload:[t]});
			});
		default:
			msg.payload=data;
			node.send(msg);   
	}
}
function processInput(node,msg){
	try{
    	switch(msg.topic) {
			case'listGroups': 
				node.connection.listGroups((err,data)=>msgProcess(node,msg,err,data));
				return;
			case'describeGroups': 
				node.connection.describeGroups(msg.payload,(err,data)=>msgProcess(node,msg,err,data));
				return;
			case'listTopics': 
				node.connection.listTopics((err,data)=>msgProcess(node,msg,err,data));
				return;
			case'createTopics': 
				//	msg.payload = [{topic: 'topic1',partitions: 1,replicationFactor: 2}];
				node.connection.createTopics(msg.payload,(err,data)=>msgProcess(node,msg,err,data));
				return;
			case'describeConfigs':
				// msg.payload={type:'topic',name:'a-topic'}
				const resource = {
					  resourceType: node.connection.RESOURCE_TYPES[msg.payload.type||'topic'],   // 'broker' or 'topic'
					  resourceName: msg.payload.name,
					  configNames: []           // specific config names, or empty array to return all,
					}
				const payload = {
					  resources: [resource],
					  includeSynonyms: false   // requires kafka 2.0+
					};
				node.connection.describeConfigs(payload, (err,data)=>msgProcess(node,msg,err,data));
   				return;
			case'refreshMetadata': 
				node.connection.refreshMetadata();
				return;
			default: throw Error("invalid topic");
    	}
	} catch(e) {
		debug({label:"input catch",error:e,msg:msg,connection:Object.keys(node.connection)});
		msg.error=e.toString();
		node.send([null,msg]);
	}

}
function adminRequest(node,res,err,data){
	if(err) {
	    node.error(err);
	    res.status(500).send(err);
		return;
	}
    res.status(200).send(data);
}
function errorWaiting(node,err) {
	while(node.waiting.length){
		let msg=node.waiting.shift();
		msg.error=err;
		node.send([null,msg]);
	}
}

module.exports = function(RED) {
    function KafkaAdminNode(n) {
        RED.nodes.createNode(this,n);
        var node=Object.assign(this,n,{connected:false,waiting:[],connected:false,connecting:false,processInput:processInput});
        node.brokerNode=RED.nodes.getNode(node.broker);
        node.brokerNode.setState(node);
   		node.status({ fill: 'yellow', shape: 'ring', text: "Deferred connection" });
   		try{
   			if(!node.brokerNode) throw Error("Broker not found "+node.broker);
            node.on('input', function (msg) {
           	if(node.connected){
                	processInput(node,msg);
            	} else {
            		node.waiting.push(msg);
            		if(node.connecting) {
                		node.log(node.waiting.length+" in wait queue");
            			return;
            		}
               		node.brokerNode.connect(node,"Admin",(err)=>{
               			node.error("connection failed, clearing waiting queue "+node.waiting.length);
               			errorWaiting(node,err);
               		});
            	}
            });
    	} catch (e) {
			node.error(e.toString());
       		node.status({ fill: 'red', shape: 'ring', text: e.message });
       		return;
    	}
		node.on("close", function(removed,done) {
       		node.status({ fill: 'red', shape: 'ring', text: "closed" });
			node.connection.close(false,()=>{
				node.log("closed");
			});
       		done();
   		});
    }
    RED.nodes.registerType("Kafka Admin",KafkaAdminNode);
    RED.httpAdmin.get("/KafkaAdmin/:id/:action/", RED.auth.needsPermission("KafkaAdmin.write"),  function(req,res) {
    	var node = RED.nodes.getNode(req.params.id);
    	if (node && node.type==="Kafka Admin") {
    		if(!node.connected) {
           		node.brokerNode.connect(node,"Admin",(err)=>{
        	        node.error(err);
        	        res.status(500).send(err);
           		});
           		return;
    		}
    	    try {
    	    	switch (req.params.action) {
    	    		case 'listGroups':
        				node.connection.listGroups((err,data)=>adminRequest(node,res,err,data));
    	       	     	break;
        			case 'describeGroups': 
        				node.connection.describeGroups(msg.payload,(err,data)=>adminRequest(node,res,err,data));
        				return;
        			case 'listTopics': 
        				node.connection.listTopics((err,data)=>adminRequest(node,res,err,data));
        				return;
        			case 'connect': 
        				debug({label:"httpadmin connect",connection:Object.keys(node.connection)});
        				node.connection.connect((err,data)=>adminRequest(node,res,err,data));
        				return;
    	       	     default:
    	       	    	 throw Error("unknown action: "+req.params.action);
    	    	}
    	    } catch(err) {
    			debug({label:"httpAdmin",error:err,request:req.params,connection:Object.keys(node.connection)});
    	    	var reason1='Internal Server Error, '+req.params.action+' failed '+err.toString();
    	        node.error(reason1);
    	        res.status(500).send(reason1);
    	    }
    	} else {
    		var reason2="request to "+req.params.action+" failed for id:" +req.params.id;
    		res.status(404).send(reason2);
    	}
    });
};