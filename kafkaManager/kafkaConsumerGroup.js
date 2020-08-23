const logger = new (require("node-red-contrib-logger"))("Kafka Consumer Group");
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");

let kafka;
function connect (node) {
	if(logger.active) logger.send({ label: 'connect', node: node.id })
	if(node.consumer) throw Error("already open");
	if(node.opening) throw Error("already opening");
	node.opening=true;
	node.status({
		fill: 'yellow',
		shape: 'ring',
		text: 'Open and wait ' + node.brokerNode.name
	})
	node.options = {
		kafkaHost: node.brokerNode.kafkaHost, // connect directly to kafka broker (instantiates a KafkaClient)
		// batch: undefined, // put client batch settings if you need them
		// ssl: true, // optional (defaults to false) or tls options hash

		groupId: node.groupId,
		sessionTimeout: node.sessionTimeout, // default: 5000,
		protocol: node.protocol, // default: ['roundrobin'],
		encoding: node.encoding, // default: utf8
		fromOffset: node.fromOffset, // default: 'latest'
		commitOffsetsOnFirstJoin: node.commitOffsetsOnFirstJoin === 'true',
		outOfRangeOffset: node.outOfRangeOffset, // default: 'earliest'
		onRebalance: (isAlreadyMember, callback) => { callback() } // or null
	}
	if (node.brokerNode.TLSOptions) {
		node.options.sslOptions = node.brokerNode.TLSOptions
	}
	node.consumer = new kafka.ConsumerGroup(node.options, node.topics)
	node.consumer.on('message', (message) => {
		if(logger.active) logger.send({ label: 'consumer.on.message', node: node.id, message: message })
		if (!node.ready) {
			node.ready = true
			node.status({ fill: 'green', shape: 'ring', text: 'Ready with ' + node.brokerNode.name })
			if (message.value == null) return //	seems to send an empty on connect in no messages waiting
		}
		if (node.timedout) {
			node.timedout = false
			node.status({ fill: 'green', shape: 'ring', text: 'Ready with ' + node.brokerNode.name })
		}
		if (Array.isArray(message)) {
			message.forEach((r) => node.brokerNode.sendMsg(node, r))
		} else {
			node.brokerNode.sendMsg(node, message)
		}
	})

	node.consumer.on('error', function (e) {
		if(logger.active) logger.send({ label: 'consumer.on.error', node: node.id, error: e })
		const err=e.message?e.message:e.toString();
		if (err.startsWith('Request timed out')) {
			node.status({ fill: 'yellow', shape: 'ring', text: e.message })
			node.log('on error ' + err)
			node.timedout = true
			return
		}
		node.error('on error ' + err)

		node.status({ fill: 'red', shape: 'ring', text: node.brokerNode.getRevisedMessage(err) })
	})
	node.consumer.on('offsetOutOfRange', function (e) {
		if(logger.active) logger.send({ label: 'consumer.on.offsetOutOfRange', node: node.id, error: e })
		node.error('on offsetOutOfRange ' + e)
		node.status({ fill: 'red', shape: 'ring', text: e.message + ' (PAUSED)' })
		node.consumer.pause()
	})
}
module.exports = function (RED) {
	function KafkaConsumerGroupNode (n) {
		RED.nodes.createNode(this, n)
		var node = Object.assign(this, n, { connected: false, paused: false, timedout: false })
		node.brokerNode = RED.nodes.getNode(node.broker)
		node.status({ fill: 'yellow', shape: 'ring', text: 'Initialising' })
		try {
			if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
			if (!kafka) {
				kafka = node.brokerNode.getKafkaDriver()
			}
			node.brokerNode.onStateUp.push({
				node: node,
				callback: function () {
					if(logger.active) logger.send({ label: 'brokerNode.stateUp', node: node.id })
					connect(node)
				}
			}) // needed due to bug in kafka driver
			node.brokerNode.stateUp.push({
				node: node,
				callback: function () {
					if(logger.active) logger.send({ label: 'brokerNode.stateUp', node: node.id })
					if (this.paused) {
						this.log('state changed to up and in paused state')
						return
					}
					if (!this.ready) {
						this.log('state changed to up but not in ready state')
						return
					}
					this.log('state changed to up, resume issued')
					this.resume()
				}
			})
			node.on('close', function (removed, done) {
				if(logger.active) logger.send({ label: 'close', node: node.id })
				node.status({ fill: 'red', shape: 'ring', text: 'closed' })
				node.consumer.close(false, () => {
					node.log('closed')
				})
				done()
			})
			node.close = (okCallback,errCallback) => node.brokerNode.closeNode(node,okCallback,errCallback);
/*			node.close = (okCallback,errCallback) => {
				if(logger.active) logger.send({
					label: 'close',
					node: node.id
				})
				try{
					if(node.consumer==null) throw Error('attempted close but already closed');
					node.consumer.close(false, () => {
						node.opening=false
						delete node.consumer
						node.log('closed')
						node.status({
							fill: 'red',
							shape: 'ring',
							text: 'Closed'
						})
					})
				} catch(ex) {
					if(errCallback) errCallback(ex);
					return
				}
				if(okCallback) okCallback();
			}
*/			node.open = (okCallback,errCallback) => {
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
				if(logger.active) logger.send({ label: 'pause', node: node.id })
				node.paused = true
				node.consumer.pause()
				node.status({ fill: 'red', shape: 'ring', text: 'paused' })
			}
			node.resume = () => {
				if(logger.active) logger.send({ label: 'resume', node: node.id })
				node.resumed = true
				node.consumer.resume()
				node.status({ fill: 'green', shape: 'ring', text: 'Ready with ' + node.brokerNode.name })
			}
			node.commit = () => {
				node.consumer.commit((err, data) => {
					if(logger.active) logger.send({ label: 'commit', node: node.id, error: err, data: data })
				})
			}
		} catch (e) {
			node.error(e.toString())
			node.status({ fill: 'red', shape: 'ring', text: e.toString() })
		}
	}
	RED.nodes.registerType(logger.label, KafkaConsumerGroupNode)
	RED.httpAdmin.get('/KafkaConsumerGroup/:id/:action/', RED.auth.needsPermission('KafkaConsumerGroup.write'), function (req, res) {
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
