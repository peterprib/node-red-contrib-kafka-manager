const ts=(new Date().toString()).split(' ');
console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [info] Kafka Broker Copyright 2019 Jaroslav Peter Prib");

const debugOff=(()=>false);
function debugOn(m) {
	const label="Kafka Broker"
	if(!debugCnt--) {
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] "+label+" debugging turn off");
		debug=debugOff;
	}
	if(debugCnt<0) {
		debugCnt=100;
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] "+label+" debugging next "+debugCnt+" debug points");
	}
	console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] "+label+" "+(m instanceof Object?JSON.stringify(m):m));
}
let debug=debugOn,debugCnt=100;


let kafka;
function hostAvailable(host, port, node, availableCB, downCB, timeoutCB) {
	debug({label:"hostAvailable",host:host, port:port, node:node.id});
	const socket=new require('net').Socket();
	socket.setTimeout(2000);
	socket.on('connect', function() {
		debug({label:"hostAvailable connect",host:host, port:port, node:node.id});
		socket.destroy();
        availableCB.apply(node,[node]);
    }).on('error', function(e) {
		debug({label:"hostAvailable error",host:host, port:port, node:node.id,error:e.toString()});
    	socket.destroy();
        downCB.apply(node,[e]);
    }).on('timeout', function() {
		debug({label:"hostAvailable timeout",host:host, port:port, node:node.id});
    	(downCB||timeoutCB).apply(node,["time out"]);
    }).connect(port, host);
};
function stateChange(list,state){
	list.forEach((r)=>{
		r.node.log("state change "+state);
		r.callback.apply(r.node,[r.args]);
	})
};
function testHost(node) {
	debug({label:"testHost",host:node.host, port:node.port, node:node.id,available:node.available});
    hostAvailable(node.host,node.port,node,
    	()=>{
       		if(node.available) return;
       		node.available=true;
       		node.log("state change up, processing "+node.stateUp.length+" node scripts");
       		stateChange(node.stateUp,"Up");
       		while(node.onStateUp.length) {
       			let r=node.onStateUp.shift();
       			r.callback.apply(r.node,[r.args]);
       		}
       	},
       	(err)=>{
       		if(!node.available) return;
       		node.available=false;
       		node.error("state change down "+err.toString());
       		stateChange(node.stateDown,"Down");
       	}
    );
}
function runtimeStop() {
	if(this.runtimeTimer) {
		clearTimeout(this.runtimeTimer);
		this.runtimeTimer=null;
	}
    this.status({fill:"red",shape:"ring",text:"Stopped"});
	this.log("Monitor Stopped");
}

function setState(node){
	node.brokerNode.stateUp.push({node:node,callback: function() {
			this.status({fill:"green",shape:"ring",text:"Available"});
		} 
	});
	node.brokerNode.stateDown.push({node:node,callback: function() {
		this.status({fill:"Red",shape:"ring",text:"Down"});
		} 
	});
}

function connect(node,type,errCB){
	debug({label:"connect",type:type,node:node,node:node.id});
	node.connecting=true;
	if(!node.brokerNode.host || !node.brokerNode.port) {
		node.connecting=false;
		node.log("host and/or port not defined");
		node.status({ fill: 'red', shape: 'ring', text: "host not defined correctly" });
		if(errCB) errCB.apply(node,["host/port missing"]);
		return;
	}
	hostAvailable(node.brokerNode.host,node.brokerNode.port,node,
		()=>{
			debug({label:"connect hostAvailable",node:node.id,host:node.brokerNode.host,port:node.brokerNode.port});
			if(node.client) {
           		node.status({ fill: 'yellow', shape: 'ring', text: "Reconnecting" });
        		node.log("Trying to reconnect");
        		node.client.connect();
        		return;
			}
			node.log("Initiating connection");
	   		node.status({ fill: 'yellow', shape: 'ring', text: "Connecting" });
			connectKafka(node,type);
		},
		(e)=>{
			debug({label:"connect hostAvailable error",node:node.id,host:node.brokerNode.host,port:node.brokerNode.port,error:e});
			node.connecting=false;
			node.status({ fill: 'red', shape: 'ring', text: "Kafka is down or unreachable" });
			if(errCB) errCB.apply(node,[e]);
		}
	);
};

function connectKafka(node,type){
	debug({label:"connectKafka",type:type,node:node.id,host:node.host,port:node.port});
	node.client = node.brokerNode.getKafkaClient();
	node.connection = new kafka[type](node.client);
	node.connecting=true;
	node.connection.on('error', function (e) {
		debug({label:"connectKafka on error",node:node.id,error:e});
		node.connected=false;
		node.connecting=false;
		const err=node.brokerNode.getRevisedMessage(e.message);
		node.error("on error "+e.message);
   		node.status({ fill: 'red', shape: 'ring', text: err });
		while(node.waiting.length){
			let msg=node.waiting.shift();
			msg.error=e.message;
			node.send([null,msg]);
		}
	});
	node.connection.on('connect', function () {
		debug({label:"connectKafka on connect",node:node.id});
		node.connected=true;
		node.connecting=false;
		debug("connected");
   		node.status({ fill: 'green', shape: 'ring', text: "Ready" });
   		node.log("connected and processing "+node.waiting.length+" messages");
   		if(node.waiting) {
   			while(node.waiting.length){
   				node.processInput.apply(node,[node,node.waiting.shift()]);
   			}
   		}
	});
}

module.exports = function(RED) {
    function KafkaBrokerNode(n) {
        RED.nodes.createNode(this,n);
        let node=Object.assign(this,n,{available:false,connect:connect,setState:setState,stateUp:[],stateDown:[],onStateUp:[]});
        node.getKafkaDriver = (()=> {
        	if(!kafka) {
        		try{
            		kafka = require('kafka-node');
        		}catch (e) {
        			node.error(e.toString());
        			throw e;
        		}
        	}
        	return kafka;
        });
        
		node.getKafkaClient = ((o)=> {
			let options=Object.assign({
   				kafkaHost: node.host+':'+node.port,
   				connectTimeout: node.connectTimeout||10000,
   				requestTimeout: node.requestTimeout||30000,
   				autoConnect: (node.autoConnect||"true")=="true",
   				idleConnection: node.idleConnection||5,
   				reconnectOnIdle: (node.reconnectOnIdle||"true")=="true",
   				maxAsyncRequests: node.maxAsyncRequests||10
				
//   			sslOptions: Object, options to be passed to the tls broker sockets, ex. { rejectUnauthorized: false } (Kafka 0.9+)
/*
var fs = require("fs");
var sslOptions = {
  key : fs.readFileSync("./rootCa.key"),
  cert : fs.readFileSync("./rootCa.crt")
};
 */
    		},o)
    		
    		if(node.credentials.has_password) {
    			options.sasl={ mechanism: 'plain', username: this.credentials.user, password: node.credentials.password };
    		}
    		
    		debug({label:"getKafkaClient",options:options});
        	return new kafka.KafkaClient(options);
        });
		node.getRevisedMessage = ((err)=> {
			if(err.startsWith("connect ECONNREFUSED")) return "Connection refused, check if Kafka up";
			return err;
        });
        testHost(node);
        if(node.checkInterval && node.checkInterval>0) {
        	this.runtimeTimer=setInterval(function(){testHost.apply(node,[node]);},node.checkInterval*1000);
            this.close = function() {
            	runtimeStop.apply(node);
            };
        }
    }
    KafkaBrokerNode.prototype.close = function() {
    	runtimeStop.apply(this);
    };
    RED.nodes.registerType("Kafka Broker",KafkaBrokerNode,{
	   credentials: {
            user: {type: "text"},
            password: {type: "password"}
       }
   });
};