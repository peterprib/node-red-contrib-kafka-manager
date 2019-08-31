const ts=(new Date().toString()).split(' ');
console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [info] Kafka Consumer Copyright 2019 Jaroslav Peter Prib");

const debugOff=(()=>false);
function debugOn(m) {
	if(!debugCnt--) {
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] Kafka Consumer debugging turn off");
		debug=debugOff;
	}
	if(debugCnt<0) {
		debugCnt=100;
		console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] Kafka Consumer debugging next "+debugCnt+" debug points");
	}
	console.log([parseInt(ts[2],10),ts[1],ts[4]].join(' ')+" - [debug] Kafka Consumer "+(m instanceof Object?JSON.stringify(m):m));
}
let debug=debugOn,debugCnt=100;

let kafka;

function ConsumerSend(node,msg) {
	node.Consumer.send([{topic: msg.topic, messages: msg.payload}],function (err, data) {
		if(err) node.error(err);
		debug("data sent "+JSON.stringify(data) );
	});
}
function sendMsg(node,message) {
	node.send({
		topic:message.topic||node.topic,
		payload:message.value,
		_kafka: {
			offset: message.offset,
			partition: message.partition,
			highWaterOffset: message.highWaterOffset,
			key: message.key
		}
	});
}
function connect(node) {
	node.consumer = new kafka[(node.connectionType||"Consumer")](node.client,
    		[
            	{ topic: node.topic, partition: 0 }
            ],
           	{	groupId: node.groupId||"kafka-node-group",
            	autoCommit: (node.autoCommit||"true")=="true",
            	autoCommitIntervalMs: node.autoCommitIntervalMs||5000,
            	fetchMaxWaitMs: node.fetchMaxWaitMs||100,
            	fetchMinBytes: node.fetchMinBytes||1,
            	fetchMaxBytes: node.fetchMaxBytes||1024 * 1024,
            	fromOffset: node.fromOffset||0,
            	encoding: node.encoding||'utf8',
            	keyEncoding: node.keyEncoding||'utf8'
         	}
       	);
      	node.consumer.on('message', (message)=>{
      		if(!node.ready) {
      			node.ready=true;
           		node.status({ fill: 'green', shape: 'ring', text: "Ready with "+ node.brokerNode.name });
           		if(message.value==null) return;  //  seems to send an empty on connect in no messages waiting
      		}
      		if(node.timedout) {
           		node.timedout=false;
           		node.status({ fill: 'green', shape: 'ring', text: "Ready with "+ node.brokerNode.name });
      		}
      		if(Array.isArray(message)) {
      			message.forEach( (r)=>sendMsg(node,r));
      		} else {
      			sendMsg(node,message);
      		}
    	});
      	
		node.consumer.on('error', function (e) {
			if(e.message.startsWith("Request timed out")) {
	       		node.status({ fill: 'yellow', shape: 'ring', text: e.message });
				node.log("on error "+e.message);
				node.timedout=true;
				return;
			}
			node.error("on error "+e.message);
			
			const err=node.brokerNode.getRevisedMessage(e.message);
       		node.status({ fill: 'red', shape: 'ring', text: err });
		})
		node.consumer.on('offsetOutOfRange', function (e) {
			node.error(e);
       		node.status({ fill: 'red', shape: 'ring', text: e.message });
		})
}
module.exports = function(RED) {
    function KafkaConsumerNode(n) {
        RED.nodes.createNode(this,n);
        var node=Object.assign(this,n,{connected:false,paused:false,timedout:false});
        node.brokerNode=RED.nodes.getNode(node.broker);
   		node.status({ fill: 'yellow', shape: 'ring', text: "Initialising" });
   		try{
   			if(!node.brokerNode) throw Error("Broker not found "+node.broker);
   			if(!kafka) {
   				kafka = node.brokerNode.getKafkaDriver();
   			}
   			if(!node.topic) throw Error("Topic is null or empty string");
      		node.client = node.brokerNode.getKafkaClient();
   			node.brokerNode.onStateUp.push({node:node,callback:function(){connect(node);}});  //needed due to bug in kafka driver
   			node.brokerNode.stateUp.push({node:node,callback: function() {
   					if(this.paused) {
						this.log("state changed to up and in paused state");
						return;
					}
   					if(!this.ready) {
						this.log("state changed to up but not in ready state");
						return;
					}
					this.log("state changed to up, resume issued");
					this.resume();
   				} 
   			});
    		node.on("close", function(removed,done) {
	       		node.status({ fill: 'red', shape: 'ring', text: "closed" });
    			node.consumer.close(false,()=>{
    				node.log("closed");
    			});
				done();
       		});
			node.pause = (()=>{
				node.paused=true;
				node.consumer.pause();
	       		node.status({ fill: 'red', shape: 'ring', text: "paused" });
			});
			node.resume = (()=>{
				node.resumed=true;
				node.consumer.resume();
           		node.status({ fill: 'green', shape: 'ring', text: "Ready with "+ node.brokerNode.name });
			});
			node.addTopics = ((topics, fromOffset)=>{
				node.consumer.addTopics(topics,
					(err, added)=>{},
					fromOffset
				);
			});
			node.removeTopics = ((topics)=>{
				node.consumer.removeTopics(topics,(err,added)=>{	
				});
			});
			node.commit = (()=>{
				node.consumer.commit((err, data)=>{});
			});
			node.setOffset = ((topic, partition, offset)=>node.consumer.setOffset(topic, partition, offset));
			node.pauseTopics = ((topics)=>node.consumer.pauseTopics(topics));
			node.resumeTopics = ((topics)=>node.consumer.resumeTopics(topics));
    	} catch (e) {
			node.error(e.toString());
       		node.status({ fill: 'red', shape: 'ring', text: e.toString() });
       		return;
    	}
    }
    RED.nodes.registerType("Kafka Consumer",KafkaConsumerNode);
    RED.httpAdmin.get("/KafkaConsumer/:id/:action/", RED.auth.needsPermission("KafkaConsumer.write"),  function(req,res) {
    	var node = RED.nodes.getNode(req.params.id);
    	if (node && node.type==="Kafka Consumer") {
    	    try {
    	    	switch (req.params.action) {
    	    		case 'pause':
    	       	    	node.pause();
    	       	     	break;
    	    		case 'resume':
    	       	    	node.resume();
    	       	     	break;
    	       	     default:
    	       	    	 throw Error("unknown action: "+req.params.action);
    	    	}
    	    	node.warn("Request to "+req.params.action);
    	        res.sendStatus(200);
    	    } catch(err) {
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