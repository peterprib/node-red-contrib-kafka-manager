const logger = new (require("node-red-contrib-logger"))("Kafka Broker");
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");
const Metadata=require("./Metadata.js");
let kafka;

function adminRequestSend(node,request) {
	if(logger.active) logger.send({label:'adminRequestSend',action:request.action,properties:Object.keys(request)})
	try {
		if(request.data) node.connection[request.action](request.data,
				(err, data)=>{
//					if(logger.active) logger.send({label:'adminRequestSend connection',error:err,data:data})
					if(err) request.error( err, data)
					else request.callback(data)
			})
		else node.connection[request.action](
				(err, data)=>{
//					if(logger.active) logger.send({label:'adminRequestSend connection',error:err,data:data})
					if(err) request.error(err, data)
					else request.callback(data)
			})
	} catch (ex) {
		logger.send({label:'adminRequestProcess',error:ex.message,stack:ex.stack});
		request.error(node,ex.message)
	}
}
function adminRequest(request) {
	if(logger.active) logger.send({label:'adminRequest',action:request.action,properties:Object.keys(request)})
	if(!request.action) throw Error("action not provided")
	if(!request.callback) throw Error("callback not provided")
	if(!request.error) throw Error("error function not provided")
	if (this.connected) {
		this.processInput(this,request) // processInput=adminRequestSend
		return;
	} 
	this.waiting.push(request)
	if (this.connecting) {
		logger.info(this.waiting.length + ' in wait queue')
		return
	}
	const node=this;
	connect(node, 'Admin', (err) => {
		logger.error('adminRequest connection failed, clearing waiting queue ' + node.waiting.length)
		while (node.waiting.length) {
			const request=node.waiting.shift()
			request.error(node,err)
		}
	})
}

function hostAvailable (host, port, node, availableCB, downCB, timeoutCB) {
  if(logger.active) logger.send({label: 'hostAvailable',host: host,port: port,node: node.id});
  const net = require('net')
  const socket = new net.Socket()
  socket.on('connect', function () {
	if(logger.active) logger.send({label:'hostAvailable connect',host:host,port:port,node: node.id})
    try {
      socket.destroy()
      availableCB.apply(node, [node])
    } catch (e) {
      node.error('hostAvailable on connect error ' + e.message)
    }
  })
    .on('end', function () {
      if(logger.active) logger.send({label: 'hostAvailable end',host: host,port: port,node: node.id});
    })
    .on('error', function (e) {
      if(logger.active) logger.send({label: 'hostAvailable error',host: host,port: port,node: node.id,error: e.toString()});
      try {
        socket.destroy()
        downCB.apply(node, [e])
      } catch (e) {
        node.error('hostAvailable on error ' + e.message)
      }
    }).on('timeout', function () {
      if(logger.active) logger.send({label:'hostAvailable timeout',host:host,port:port,node:node.id});
      try {
        (downCB || timeoutCB).apply(node, ['time out'])
      } catch (e) {
        node.error('hostAvailable on error ' + e.message)
      }
    })
  socket.setTimeout(2000)
  socket.connect(port, host)
};

function stateChange (list, state) {
  list.forEach((r) => {
    r.node.log('state change ' + state)
    r.callback.apply(r.node, [r.args])
  })
};

function testHosts (node) {
  if(logger.active) logger.send({label:'testHosts',node:node.id,available: node.available});
  testHost(node, 0)
}

function testHost (node, i) {
  if (i >= node.hostsCombined.length) {
    if (!node.available) return
    node.available = false
    node.error('state change down')
    stateChange(node.stateDown, 'Down')
    return
  }
  const host = node.hostsCombined[i]
  if(logger.active) logger.send({label:'testHost',host:host.host,port:host.port,node:node.id});
  hostAvailable(host.host, host.port, node,
    () => {
      if (node.available) return
      node.available = true
      node.log('state change up, processing ' + node.stateUp.length + ' node scripts')
      stateChange(node.stateUp, 'Up')
      while (node.onStateUp.length) {
        const r = node.onStateUp.shift()
        r.callback.apply(r.node, [r.args])
      }
    },
    () => {
      testHost(node, ++i)
    }
  )
}

function runtimeStop () {
  if (this.runtimeTimer) {
//	logger.info("stopped runtime timer")
    clearTimeout(this.runtimeTimer)
    this.runtimeTimer = null
  }
  this.status({fill:'red',shape:'ring',text:'Stopped'})
  this.log('Monitor Stopped')
}

function sendMsg (node, message) {
	if(logger.active) logger.send({label: 'sendMsg',node: node.id,message: message	})
	const kafka={
		offset: message.offset,
		partition: message.partition,
		highWaterOffset: message.highWaterOffset,
		key: message.key,
		commit:(node.autoCommitBoolean?(callback)=>callback():
			(callback,callbackError)=>{
				node.consumer.commit((err, data)=>{
					if(err){
						callbackError(err)
					}else{
						callback();
					}
				})
			}),
		rollback:(node.autoCommitBoolean?(callback)=>callback():
			(callback,callbackError)=>{
				logger.sendWarning("rollback close");
				node.close(callback,callbackError);
			})
	};
	node.send({
		topic: message.topic || node.topic,
		payload: message.value,
		_kafka: kafka
	})
}

function setState (node) {
  node.brokerNode.stateUp.push({
    node: node,
    callback: function () {
      this.status({fill:'green',shape:'ring',text: 'Available'})
    }
  })
  node.brokerNode.stateDown.push({
    node: node,
    callback: function () {
      this.status({fill:'Red',shape:'ring',text: 'Down'})
    }
  })
}

function connect (node, type, errCB) {
  if(logger.active) logger.send({label:'connect',type:type,node:node.id,available:node.available});
  if (node.available) {
    if (errCB) errCB.apply(node, ['connect attempt and not available'])
    return
  }
  if (!node.brokerNode.hostsCombined.length) {
    node.connecting = false
    node.log('host and/or port not defined')
    node.status({fill:'red',shape:'ring',text:'host not defined correctly'})
    if (errCB) errCB.apply(node, ['host/port missing'])
    return
  }
  node.connecting = true
  if (node.client) {
	  node.status({fill: 'yellow',shape: 'ring',text: 'reconnecting'})
	  node.log('Trying to reconnect')
	  node.client.connect()
	  return
  }
  node.log('Initiating connection')
  node.status({fill: 'yellow',shape: 'ring',text: 'Connecting'})
  connectKafka(node, type)
};

function connectKafka (node, type) {
  if(logger.active) logger.send({label:'connectKafka',type:type,node:node.id,host: node.host,port:node.port});
  node.client = node.brokerNode.getKafkaClient()
  node.connection = new kafka[type](node.client)
  node.connecting = true
  node.connection.on('error', function (e) {
	if(logger.active) logger.send({label: 'connectKafka on error',node: node.id,error: e});
    node.connected = false
    node.connecting = false
    const err = node.brokerNode.getRevisedMessage(e.message)
    node.error('on error ' + e.message)
    node.status({fill: 'red',shape: 'ring',text: err})
    while (node.waiting.length) {
      const msg = node.waiting.shift()
      msg.error = e.message
      node.send([null, msg])
    }
  })
  node.connection.on('connect', function () {
	if(logger.active) logger.send({label: 'connectKafka on connect', node: node.id});
    node.connected = true
    node.connecting = false
    if(logger.active) logger.send('connected');
    node.brokerNode.onConnectStack.forEach(callFunction=>callFunction())
    node.status({fill: 'green',shape: 'ring',text: 'Ready'})
    node.log('connected and processing ' + node.waiting.length + ' messages')
    if (node.waiting) {
      while (node.waiting.length) node.processInput.apply(node, [node, node.waiting.shift()])
    }
  })
}

function closeNode(node,okCallback,errCallback) {
	if(logger.active) logger.send({label: 'close',node: node.id,name: node.name})
	try{
		if(node.consumer==null) throw Error('attempted close but already closed');
		if(node.closing) throw Error('attempted close but already closing');
		node.status({fill: 'red',shape: 'ring',text: 'Closing'})
		node.consumer.close(false, () => {
			node.opening=false
			delete node.closing
			delete node.consumer
			node.log('closed')
			node.status({fill: 'red',shape: 'ring',text: 'Closed'})
		})
	} catch(ex) {
		if(errCallback) errCallback(ex);
		return
	}
	if(okCallback) okCallback();
}

module.exports = function (RED) {
  function KafkaBrokerNode (n) {
    RED.nodes.createNode(this, n)
    const node = Object.assign(this, {
      hosts: []
    }, n, {
      available: false,
      closeNode: closeNode,
      connect: connect,
      brokerNode:this,
      onStateUp: [],
      onConnectStack:[],
      onCloseStack:[],
      sendMsg:sendMsg,
      setState: setState,
      stateUp: [],
      stateDown: [],
      adminRequest:adminRequest.bind(this), 
      processInput:adminRequestSend.bind(this), // adminRequest emulate normal node   
      waiting:[]   
    })
    node.hostsCombined=[];
    if(node.hostsEnvVar) {
        if(node.hostsEnvVar in process.env) {
        	try{
            	const hosts=JSON.parse(process.env[node.hostsEnvVar]);
            	if(hosts) {
            		logger.send({label:"test",hosts:hosts});
            		if(hosts instanceof Array) {
            			node.hostsCombined=node.hostsCombined.concat(hosts);
            		} else {
            			throw Error("not array value: "+process.env[node.hostsEnvVar]);
            		}
            	}
        	} catch(e) {
    			const error="process.env variable "+node.hostsEnvVar+e.toString();
    			throw Error(error);
        	}
        } else {
        	const error="process.env."+node.hostsEnvVar+" not found ";
        	node.warn(error);
            node.status({fill: 'red',shape: 'ring',text: error});
        }
    }
    if (node.hosts.length === 0 && node.host) {
      node.hosts.push({host: node.host, port: node.port})
    }
    node.hostsCombined=node.hostsCombined.concat(node.hosts);
    logger.send({hosts:node.hostsCombined});
    node.kafkaHost = node.hostsCombined.map((r) => r.host + ':' + r.port).join(',')

    node.getKafkaDriver = () => {
      if (!kafka) {
        try {
          kafka = require('kafka-node')
        } catch (ex) {
          node.error(ex.toString())
          throw ex
        }
      }
      return kafka
    }

    node.getKafkaClient = (o) => {
      const options = Object.assign({
        kafkaHost: node.kafkaHost,
        connectTimeout: node.connectTimeout || 10000,
        requestTimeout: node.requestTimeout || 30000,
        autoConnect: (node.autoConnect || 'true') === 'true',
        idleConnection: node.idleConnection || 5,
        reconnectOnIdle: (node.reconnectOnIdle || 'true') === 'true',
        maxAsyncRequests: node.maxAsyncRequests || 10,
        useCredentials: node.useCredentials || false
      }, o)
   	  if(logger.active) logger.send({label:'getKafkaClient',usetls:node.usetls,options:options});
      if(node.usetls) {
    	  options.sslOptions = {rejectUnauthorized:node.selfSign};
      	  if(logger.active) logger.send({label:'getKafkaClient tls use',selfSign:node.selfSign});
    	  try {
    		  if(!(node.selfServe||node.tls)) throw Error("not self serve or no tls configuration selected");
    		  if(node.tls) {
        		  node.tlsNode = RED.nodes.getNode(node.tls);
     	  	      if (!node.tlsNode) throw Error("tls configuration not found");
      	  	      Object.assign(options.sslOptions,node.tlsNode.credentials);
      	  	      if(logger.active) logger.send({label:'getKafkaClient sslOptions',properties:Object.keys(options.sslOptions)});
    		  }
    	  } catch(e) {
    		  node.error("get node tls "+node.tls+" failed, error:"+e);
    	  }
      }
      if (options.useCredentials) {
    	if(logger.active) logger.send({label:'getKafkaClient node has configured credentials, note sasl mechanism is plain'});
        options.sasl = {
          mechanism: 'plain',
          username: this.credentials.user,
          password: node.credentials.password
        }
      }
      node.getKafkaDriver()
      if(logger.active) logger.send({label:'getKafkaClient',options:Object.assign({},options,options.sslOptions?{sslOptions:"***masked***"}:null)});
      return new kafka.KafkaClient(options)
    }
    node.getRevisedMessage = (err) => {
      if (typeof err === 'string' && err.startsWith('connect ECONNREFUSED')) return 'Connection refused, check if Kafka up'
      return err
    }
    testHosts(node)
    if (node.checkInterval && node.checkInterval > 0) {
    	node.runtimeTimer = setInterval(function () {
          try {
            testHosts.apply(node, [node])
          } catch (e) {
            node.send('runtimeTimer ' + e.message)
          }
        },
        node.checkInterval * 1000
      )
      node.close = function (removed, done) {
    	  logger.info("close")
   	    node.onCloseStack.forEach(callFunction=>callFunction())
        runtimeStop.apply(node)
        if(done) done()
      }
    }
    
    try{
        node.metadata=new Metadata(node,logger);
        node.onConnectStack.push(node.metadata.startRefresh);
        node.onCloseStack.push(node.metadata.stopRefresh);
        node.onChangeMetadata=node.metadata.onChange.bind(node.metadata);
        node.metadataRefresh=node.metadata.refresh.bind(node.metadata);
        node.getTopicsPartitions=node.metadata.getTopicsPartitions.bind(node.metadata);
    } catch(ex){
    	logger.error({label:"metadata",error:ex.message,stack:ex.stack})
    }
  }
  KafkaBrokerNode.prototype.close = function (removed, done) {
    runtimeStop.apply(this);
    done()
  };
  RED.nodes.registerType(logger.label, KafkaBrokerNode, {
    credentials: {
      user: {
        type: 'text'
      },
      password: {
        type: 'password'
      }
    }
  });
}
