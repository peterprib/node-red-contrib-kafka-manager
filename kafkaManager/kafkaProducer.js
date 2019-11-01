const nodeLabel="Kafka Producer";
const ts=(new Date().toString()).split(' ');
console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [info] "+nodeLabel+" Copyright 2019 Jaroslav Peter Prib");

const debugOff=(()=>false);
function debugOn(m) {
	const ts=(new Date().toString()).split(' ');
	if(!debugCnt--) {
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] "+nodeLabel+" debugging turn off");
		debug=debugOff;
	}
	if(debugCnt<0) {
		debugCnt=100;
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] "+nodeLabel+" debugging next "+debugCnt+" debug points");
	}
	console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] "+nodeLabel+" "+(m instanceof Object?JSON.stringify(m):m));
}
let debug=debugOn,debugCnt=100;


let kafka;

function producerSend(node,msg,retry) {
	debug({label:"producerSend",node:node.id,retry:retry});
//        node.KeyedMessage = kafka.KeyedMessage
//  km = new KeyedMessage('key', 'message'),
//  payloads = [
//      { topic: 'topic1', messages: 'hi', partition: 0 },
//      { topic: 'topic2', messages: ['hello', 'world', km] }
//  ];
/*

{
   topic: 'topicName',
   messages: ['message body'], // multi messages should be a array, single message can be just a string or a KeyedMessage instance
   key: 'theKey', // string or buffer, only needed when using keyed partitioner
   partition: 0, // default 0
   attributes: 2, // default: 0
   timestamp: Date.now() // <-- defaults to Date.now() (only available with kafka v0.10+)
}

attributes:
0: No compression
1: Compress using GZip
2: Compress using snappy
 */
	try{
		const topic=[{
			topic: msg.topic||node.topic||"",
			messages: msg.payload,
			key:msg.key||node.key,
			partition:msg.partition||node.partition||0,
			attributes:msg.key||node.attributes||0
		}];
		node.producer.send(topic,
			function (err, data) {
				if(err) {
					let errmsg = (typeof err == "string" ?err:err.message);
					if(errmsg.startsWith("Broker not available")) {
			            node.connected=false;
			            node.inError=true;
			            node.queueMsg(msg);
					    if(retry) {
					    	setInError(node,"retry failed");
					    	return;
					    }
						node.producer.refreshMetadata(topic, (err) => {
						    if (err) {
						    	setInError(node,errmsg);
						    	return;
						    }
						    if(node.waiting.length) {
						   		node.status({ fill: 'yellow', shape: 'ring', text: "trying sending "+node.waiting.length+" queued messages"});
						    }
						    producerSend(node,node.waiting,(retry||1));
						    if(node.waiting.length) {
						   		node.status({ fill: 'red', shape: 'ring', text: "retry send to Kafka failed, "+node.waiting.length+" queued messages"});
						    } else {
								node.status({ fill: 'green', shape: 'ring', text: "Connected to "+ node.brokerNode.name });
						    }
						});
					}
					setInError(node,errmsg);
				}
		});
		if(node.inError) {
	   		node.inError=false;
	   		node.status({ fill: 'green', shape: 'ring', text: "Ready" });
		}
	} catch(e) {
   		node.inError=true;
		node.error(e);
   		node.status({ fill: 'yellow', shape: 'ring', text: "send error "+e.toString() });
	}
}
function setInError(node,errmsg) {
	node.error(errmsg);
	node.status({ fill: 'yellow', shape: 'ring', text: "send error "+errmsg });
	node.inError=true;
}

function connect(node) {
	debug({label:"connect",node:node.id});
	if(!node.client) node.client = node.brokerNode.getKafkaClient();
	node.producer = new kafka[(node.connectionType||"Producer")](node.client,
		{
				// Configuration for when to consider a message as acknowledged, default 1
		requireAcks: node.partitionerType||1,
			    // The amount of time in milliseconds to wait for all acks before considered, default 100ms
		ackTimeoutMs: node.partitionerType||100,
			    // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
		partitionerType: node.partitionerType||0
	});
	node.status({ fill: 'yellow', shape: 'ring', text: "Waiting on "+ node.brokerNode.name });
	node.producer.on('error', function (e) {
		node.error("on error "+e.message);
		const err=node.brokerNode.getRevisedMessage(e.message);
		node.status({ fill: 'red', shape: 'ring', text: err });
	})
	node.producer.on('ready', function () {
		node.status({ fill: 'green', shape: 'ring', text: "Connected to "+ node.brokerNode.name });
		node.connected=true;
		node.log("connected and processing "+node.waiting.length+" messages");
		producerSend(node,node.waiting);
	});
}

module.exports = function(RED) {
    function KafkaProducerNode(n) {
        RED.nodes.createNode(this,n);
        let node=Object.assign(this,n,{connected:false,waiting:[]});
        node.brokerNode=RED.nodes.getNode(node.broker);
   		node.status({ fill: 'yellow', shape: 'ring', text: "Initialising" });
   		try{
   			if(!node.brokerNode) throw Error("Broker not found "+node.broker);
   			if(!kafka) {
   				kafka = node.brokerNode.getKafkaDriver();
   			}
   			node.brokerNode.onStateUp.push({node:node,callback:function(){connect(node);}});  //needed due to bug in kafka driver
    	} catch (e) {
			node.error(e.message);
       		node.status({ fill: 'red', shape: 'ring', text: e.message });
       		return;
    	}
        node.queueMsg = function(msg) {
            if(!node.waiting.length) {
            	const warning="Connection down started queuing messages";
    			node.warn(warning);
           		node.status({ fill: 'red', shape: 'ring', text: warning});
            }
        	node.waiting.push(msg);
            if(!(node.waiting.length%100)){
            	const warning="Connection down, queue depth reached "+node.waiting.length;
    			node.warn(warning);
           		node.status({ fill: 'red', shape: 'ring', text: warning});
            }
        }
        node.on('input', function (msg) {
            if(node.connected) {
            	producerSend(node,msg);
            	return;
            }
            node.queueMsg(msg);
        });
		node.on("close", function(removed,done) {
       		node.status({ fill: 'red', shape: 'ring', text: "closed" });
			node.producer.close(false,()=>{
				node.log("closed");
			});
       		done();
   		});
    }
    RED.nodes.registerType(nodeLabel,KafkaProducerNode);
};