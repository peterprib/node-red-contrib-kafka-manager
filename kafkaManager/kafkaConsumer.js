const logger = new (require("node-red-contrib-logger"))("Kafka Consumer");
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");

let kafka;
function connect (node) {
	if(logger.active) logger.send({
		label: 'connect',
		node: node.id
	})
	if(node.consumer) throw Error("already open");
	if(node.opening) throw Error("already opening");
	node.opening=true;
	node.status({
		fill: 'yellow',
		shape: 'ring',
		text: 'Open and wait ' + node.brokerNode.name
	})
	node.client = node.brokerNode.getKafkaClient()
	node.consumer = new kafka[(node.connectionType || 'Consumer')](node.client, node.topics, {
		groupId: node.groupId || 'kafka-node-group',
		autoCommit: node.autoCommitBoolean,
		autoCommitIntervalMs: node.autoCommitIntervalMs,
		fetchMaxWaitMs: node.fetchMaxWaitMs,
		fetchMinBytes: node.fetchMinBytes,
		fetchMaxBytes: node.fetchMaxBytes,
		fromOffset: node.fromOffset,
		encoding: node.encoding,
		keyEncoding: node.keyEncoding
	})
	node.consumer.on('message', (message) => {
		if(logger.active) logger.send({
			label: 'consumer.on.message',
			node: node.id,
			message: message
		})
		if (!node.ready) {
			node.ready = true
			node.status({
				fill: 'green',
				shape: 'ring',
				text: 'Ready with ' + node.brokerNode.name
			})
			if (message.value == null) return //	seems to send an empty on connect in no messages waiting
		}
		if (node.timedout) {
			node.timedout = false
			node.status({
				fill: 'green',
				shape: 'ring',
				text: 'Ready with ' + node.brokerNode.name
			})
		}

		const sendMessage = function(node, message) {
			if(node.convertToJson){
				message.value = JSON.parse(message.value);
			} 
			node.brokerNode.sendMsg(node, message)
		}
		if (Array.isArray(message)) {
			message.forEach((r) => sendMessage(node, r))
		} else {
			sendMessage(node, message)
		}
	})

	node.consumer.on('error', function (e) {
		if(logger.active) logger.send({
			label: 'consumer.on.error',
			node: node.id,
			error: e
		})
		const err=e.message?e.message:e.toString();

		if (err.startsWith('Request timed out')) {
			node.status({
				fill: 'yellow',
				shape: 'ring',
				text: err
			})
			node.log('on error ' + err)
			node.timedout = true
			return
		}
		node.error('on error ' + err)

		node.status({
			fill: 'red',
			shape: 'ring',
			text: node.brokerNode.getRevisedMessage(err)
		})
	})
	node.consumer.on('offsetOutOfRange', function (ex) {
		if(logger.active) logger.send({
			label: 'consumer.on.offsetOutOfRange',
			node: node.id,
			error: ex
		})
		node.error('on offsetOutOfRange ' + ex)
		node.status({
			fill: 'red',
			shape: 'ring',
			text: ex.message + ' (PAUSED)'
		})
		node.consumer.pause()
	})
}
module.exports = function (RED) {
	function KafkaConsumerNode (n) {
		RED.nodes.createNode(this, n)
		let node=Object.assign(this,{
			checkInterval:1000,
			autoCommitIntervalMs: 5000,
			fetchMaxWaitMs: 100,
			fetchMinBytes: 1,
			fetchMaxBytes: 1024 * 1024,
			fromOffset: 0,
			encoding: 'utf8',
			keyEncoding: 'utf8',
			connected: false,
			paused: false,
			timedout: false
		},n,);
		node.autoCommitBoolean=(node.autoCommit || 'true') === 'true';
		node.brokerNode = RED.nodes.getNode(node.broker)
		node.status({
			fill: 'yellow',
			shape: 'ring',
			text: 'Initialising'
		})
		try {
			if (!node.topics) {
				node.topics = [{
					topic: node.topic,
					partition: 0
				}]
			} // legacy can be removed in future
			if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
			if (!kafka) {
				kafka = node.brokerNode.getKafkaDriver()
			}
			node.brokerNode.onStateUp.push({
				node: node,
				callback: function () {
					if(logger.active) logger.send({
						label: 'brokerNode.stateUp',
						node: node.id
					})
					connect(node);
				}
			}) // needed due to bug in kafka driver
			node.brokerNode.stateUp.push({
				node: node,
				callback: function () {
					if(logger.active) logger.send({
						label: 'brokerNode.stateUp',
						node: node.id
					})
					if (this.paused) {
						this.log('state changed to up and in paused state');
						return
					}
					if (!this.ready) {
						this.log('state changed to up but not in ready state');
						return
					}
					this.log('state changed to up, resume issued');
					this.resume()
				}
			})
			node.on('close', function (removed, done) {
				if(logger.active) logger.send({
					label: 'on.close',
					node: node.id
				})
				node.status({
					fill: 'red',
					shape: 'ring',
					text: 'closed'
				})
//				if(node.releaseStale) clearInterval(node.releaseStale);
				node.consumer.close(false, () => {
					delete node.consumer
					node.log('closed')
				})
				done()
			})
//			if(!node.autoCommitBoolean){
//				node.releaseStale = setInterval(function(node) {releaseStale(node)}, 1000*60,node);
//			}
			node.close = (okCallback,errCallback) => node.brokerNode.closeNode(node,okCallback,errCallback);
			node.open = (okCallback,errCallback) => {
				if(logger.active) logger.send({
					label: 'open',
					node: node.id
				})
				try{
					if(node.brokerNode.available!==true) throw Error("broker "+node.brokerNode.name+" available: "+node.brokerNode.available);
					connect(node);
				} catch(ex) {
					if(errCallback) errCallback(ex);
					return
				}
				if(okCallback) okCallback();
			}
			node.pause = () => {
				if(logger.active) logger.send({
					label: 'pause',
					node: node.id
				})
				node.paused = true
				node.consumer.pause()
				node.status({
					fill: 'red',
					shape: 'ring',
					text: 'paused'
				})
			}
			node.resume = () => {
				if(logger.active) logger.send({
					label: 'resume',
					node: node.id
				})
				node.resumed = true
				node.consumer.resume()
				node.status({
					fill: 'green',
					shape: 'ring',
					text: 'Ready with ' + node.brokerNode.name
				})
			}
			node.addTopics = (topics, fromOffset) => {
				node.consumer.addTopics(topics,
					(err, added) => {
						if(logger.active) logger.send({
							label: 'consumer.addTopics',
							node: node.id,
							topics: topics,
							fromOffset: fromOffset,
							added: added,
							error: err
						})
					},
					fromOffset
				)
			}
			node.removeTopics = (topics) => {
				node.consumer.removeTopics(topics, (err, removed) => {
					if(logger.active) logger.send({
						label: 'consumer.addTopics',
						node: node.id,
						topics: topics,
						removed: removed,
						error: err
					})
				})
			}
			node.commit = () => {
				node.consumer.commit((err, data) => {
					if(logger.active) logger.send({
						label: 'commit',
						node: node.id,
						error: err,
						data: data
					})
				})
			}
			node.setOffset = (topic, partition, offset) => node.consumer.setOffset(topic, partition, offset)
			node.pauseTopics = (topics) => node.consumer.pauseTopics(topics)
			node.resumeTopics = (topics) => node.consumer.resumeTopics(topics)
		} catch (e) {
			node.error(e.toString())
			node.status({
				fill: 'red',
				shape: 'ring',
				text: e.toString()
			})
		}
	}
	RED.nodes.registerType(logger.label, KafkaConsumerNode)
	RED.httpAdmin.get('/KafkaConsumer/:id/:action/', RED.auth.needsPermission('KafkaConsumer.write'), function (req, res) {
		var node = RED.nodes.getNode(req.params.id)
		if (node && node.type === logger.label) {
			try {
				switch (req.params.action) {
					case 'close':
						node.close(()=>res.sendStatus(200),(ex)=>{const err="close error: "+ex.message;node.warn(err);res.status(500).send(err)})
						return
					case 'open':
						node.open(()=>res.sendStatus(200),(ex)=>{const err="open error: "+ex.message;node.warn(err);res.status(500).send(err)})
						return;
					case 'pause':
						node.pause()
						break
					case 'resume':
						node.resume()
						break
					default:
						throw Error('unknown action: ' + req.params.action)
				}
				node.warn('Request to ' + req.params.action)
				res.sendStatus(200)
			} catch (err) {
				var reason1 = 'Internal Server Error, ' + req.params.action + ' failed ' + err.toString()
				node.error(reason1)
				res.status(500).send(reason1)
			}
		} else {
			var reason2 = 'request to ' + req.params.action + ' failed for id:' + req.params.id
			res.status(404).send(reason2)
		}
	})
}
