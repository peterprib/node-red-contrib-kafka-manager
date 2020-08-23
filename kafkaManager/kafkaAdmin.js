const logger = new (require("node-red-contrib-logger"))("Kafka Admin");
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");

function msgProcess (node, msg, errObject, data) {
	if(logger.active) logger.send({
		label: 'msgProcess',
		error: errObject,
		data: data
	});
	if (errObject) {
		const err = typeof errObject !== 'string' ? errObject.toString() : errObject.message.toString()
		if (err.startsWith('Broker not available') || err.startsWith('Request timed out')) {
			node.warn('Broker not available, queue message and retry connection')
			node.waiting.push(msg)
			node.brokerNode.connect(node, 'Admin', (err) => {
				node.error('connection failed, clearing waiting queue ' + node.waiting.length)
				errorWaiting(node, err)
			})
			return
		}
		node.error(msg.topic + ' ' + err)
		msg.error = err
		node.send([null, msg])
		return
	}
	switch (msg.topic) {
		case 'createAcls':
		case 'createTopics':
		case 'deleteAcls':
		case 'deleteTopics':
		case 'electPreferredLeaders':
			data.forEach((c, i, a) => {
				const t = msg.payload.find((cp) => cp.topic === c.topic)
				if(logger.active) logger.send({
					label: 'msgProcess multi response',
					topic: c,
					data: t
					});
				if (c.hasOwnProperty('error')) {
					if(logger.active) logger.send({
						label: 'msgProcess multi response',
						data: {
							topic: msg.topic,
							error: formatError(c.error),
							payload: [t]
						}
					});
					node.send([null, {
						topic: msg.topic,
						error: formatError(c.error),
						payload: [t]
					}])
					return
				}
				if(logger.active) logger.send({
					label: 'msgProcess multi response ok',
					data: {
						topic: msg.topic,
						payload: [c]
					}
				});
				node.send({
					topic: msg.topic,
					payload: [t]
				})
			})
			break
		default:
			msg.payload = data
			node.send(msg)
	}
}
const processInputNoArg = [
	'describeCluster', 'describeDelegationToken', 'describeReplicaLogDirs', 'listConsumerGroups', 'listGroups', 'listTopics'
]
const processInputPayloadArg = [
	'alterConfigs', 'alterReplicaLogDirs', 'createAcls', '', 'createDelegationToken',
	'createPartitions', 'createTopics', 'deleteAcls', 'deleteConsumerGroups', 'deleteRecords',
	'deleteTopics', 'describeAcls', 'describeConsumerGroups',
	'describeGroups', 'describeLogDirs', 'describeTopics', 'electPreferredLeaders',
	'expireDelegationToken', 'incrementalAlterConfigs', 'listConsumerGroupOffsets',
	'renewDelegationToken'
]

function processInput (node, msg) {
	if(logger.active) logger.send({label:'processInput',msg})
	try {
		if (processInputNoArg.includes(msg.topic)) {
			if(logger.active) logger.send({label:'processInput processInputNoArg',msg});
			node.connection[msg.topic]((err, data) => msgProcess(node, msg, err, data))
			return
		}
		if (processInputPayloadArg.includes(msg.topic)) {
			if(logger.active) logger.send({label: 'processInput processInputPayloadArg',msg});
			node.connection[msg.topic](msg.payload, (err, data) => msgProcess(node, msg, err, data))
			return
		}
		let resource = {}
		let payload = {}
		switch (msg.topic) {
			case 'describeConfigs':
				// msg.payload={type:'topic',name:'a-topic'}
				resource = {
					resourceType: node.connection.RESOURCE_TYPES[msg.payload.type || 'topic'], // 'broker' or 'topic'
					resourceName: msg.payload.name,
					configNames: [] // specific config names, or empty array to return all,
				}
				payload = {
					resources: [resource],
					includeSynonyms: false // requires kafka 2.0+
				}
				node.connection.describeConfigs(payload, (err, data) => msgProcess(node, msg, err, data))
				break
			default:
				throw Error('invalid message topic')
		}
	} catch (e) {
		if(logger.active) logger.send({
					label: 'processInput catch',
					error: e,
					msg: msg,
					connection: Object.keys(node.connection)
			});
		msg.error = e.toString()
		node.send([null, msg])
	}
}

function adminRequest (node, res, err, data) {
	if (err) {
		node.error(err)
		res.status(500).send(err)
		return
	}
	res.status(200).send(data)
}

function errorWaiting (node, err) {
	while (node.waiting.length) {
		const msg = node.waiting.shift()
		msg.error = err
		node.send([null, msg])
	}
}

module.exports = function (RED) {
	function KafkaAdminNode (n) {
		RED.nodes.createNode(this, n)
		var node = Object.assign(this, n, {
			connected: false,
			waiting: [],
			connecting: false,
			processInput: processInput
		})
		node.brokerNode = RED.nodes.getNode(node.broker)
		node.brokerNode.setState(node)
		node.status({
			fill: 'yellow',
			shape: 'ring',
			text: 'Deferred connection'
		})
		try {
			if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
			node.on('input', function (msg) {
				if (node.connected) {
					processInput(node, msg)
				} else {
					node.waiting.push(msg)
					if (node.connecting) {
						node.log(node.waiting.length + ' in wait queue')
						return
					}
					node.brokerNode.connect(node, 'Admin', (err) => {
						node.error('connection failed, clearing waiting queue ' + node.waiting.length)
						errorWaiting(node, err)
					})
				}
			})
		} catch (e) {
			node.error(e.toString())
			node.status({
				fill: 'red',
				shape: 'ring',
				text: e.message
			})
			return
		}
		node.on('close', function (removed, done) {
			node.status({
				fill: 'red',
				shape: 'ring',
				text: 'closed'
			})
			node.connection.close(false, () => {
				node.log('closed')
			})
			clearInterval(node.check);
			done()
		});
	}
	RED.nodes.registerType(logger.label, KafkaAdminNode)
	RED.httpAdmin.get('/KafkaAdmin/:id/:action/', RED.auth.needsPermission('KafkaAdmin.write'), function (req, res) {
		var node = RED.nodes.getNode(req.params.id)
		if (node && node.type === 'Kafka Admin') {
			if (!node.connected) {
				node.brokerNode.connect(node, 'Admin', (err) => {
					node.error(err)
					res.status(500).send(err)
				})
				return
			}
			try {
				if (processInputNoArg.includes(req.params.action)) {
					node.connection[req.params.action]((err, data) => adminRequest(node, res, err, data))
					return
				}
				throw Error('unknown action: ' + req.params.action)
			} catch (err) {
			if(logger.active) logger.send({
						label: 'httpAdmin',
						error: err,
						request: req.params,
						connection: Object.keys(node.connection)
					});
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
function formatError(error) {
	if(error.startsWith("received error code ")){
//received error code 37 for topic
		const errorNumber=error.split(" ")[3];
		if(errorNumber in errors) {
			return errors[errorNumber];
		}
	}
	return error;
}
const errors={
		"-1":"The server experienced an unexpected error when processing the request.",
		"0":"",
		"1":"The requested offset is not within the range of offsets maintained by the server.",
		"2":"This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
		"3":"This server does not host this topic-partition.",
		"4":"The requested fetch size is invalid.",
		"5":"There is no leader for this topic-partition as we are in the middle of a leadership election.",
		"6":"For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.",
		"7":"The request timed out.",
		"8":"The broker is not available.",
		"9":"The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
		"10":"The request included a message larger than the max message size the server will accept.",
		"11":"The controller moved to another broker.",
		"12":"The metadata field of the offset request was too large.",
		"13":"The server disconnected before a response was received.",
		"14":"The coordinator is loading and hence can,t process requests.",
		"15":"The coordinator is not available.",
		"16":"This is not the correct coordinator.",
		"17":"The request attempted to perform an operation on an invalid topic.",
		"18":"The request included message batch larger than the configured segment size on the server.",
		"19":"Messages are rejected since there are fewer in-sync replicas than required.",
		"20":"Messages are written to the log, but to fewer in-sync replicas than required.",
		"21":"Produce request specified an invalid value for required acks.",
		"22":"Specified group generation id is not valid.",
		"23":"The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
		"24":"The configured groupId is invalid.",
		"25":"The coordinator is not aware of this member.",
		"26":"The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
		"27":"The group is rebalancing, so a rejoin is needed.",
		"28":"The committing offset data size is not valid.",
		"29":"Topic authorization failed.",
		"30":"Group authorization failed.",
		"31":"Cluster authorization failed.",
		"32":"The timestamp of the message is out of acceptable range.",
		"33":"The broker does not support the requested SASL mechanism.",
		"34":"Request is not valid given the current SASL state.",
		"35":"The version of API is not supported.",
		"36":"Topic with this name already exists.",
		"37":"Number of partitions is below 1.",
		"38":"Replication factor is below 1 or larger than the number of available brokers.",
		"39":"Replica assignment is invalid.",
		"40":"Configuration is invalid.",
		"41":"This is not the correct controller for this cluster.",
		"42":"This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
		"43":"The message format version on the broker does not support the request.",
		"44":"Request parameters do not satisfy the configured policy.",
		"45":"The broker received an out of order sequence number.",
		"46":"The broker received a duplicate sequence number.",
		"47":"Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.",
		"48":"The producer attempted a transactional operation in an invalid state.",
		"49":"The producer attempted to use a producer id which is not currently assigned to its transactional id.",
		"50":"The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).",
		"51":"The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.",
		"52":"Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.",
		"53":"Transactional Id authorization failed.",
		"54":"Security features are disabled.",
		"55":"The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
		"56":"Disk error when trying to access log file on the disk.",
		"57":"The user-specified log directory is not found in the broker config.",
		"58":"SASL Authentication failed.",
		"59":"This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
		"60":"A partition reassignment is in progress.",
		"61":"Delegation Token feature is not enabled.",
		"62":"Delegation Token is not found on server.",
		"63":"Specified Principal is not valid Owner/Renewer.",
		"64":"Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
		"65":"Delegation Token authorization failed.",
		"66":"Delegation Token is expired.",
		"67":"Supplied principalType is not supported.",
		"68":"The group is not empty.",
		"69":"The group id does not exist.",
		"70":"The fetch session ID was not found.",
		"71":"The fetch session epoch is invalid.",
		"72":"There is no listener on the leader broker that matches the listener on which metadata request was processed.",
		"73":"Topic deletion is disabled.",
		"74":"The leader epoch in the request is older than the epoch on the broker.",
		"75":"The leader epoch in the request is newer than the epoch on the broker.",
		"76":"The requesting client does not support the compression type of given partition.",
		"77":"Broker epoch has changed.",
		"78":"The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.",
		"79":"The group member needs to have a valid member id before actually entering a consumer group.",
		"80":"The preferred leader was not available.",
		"81":"The consumer group has reached its max size.",
		"82":"The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.",
		"83":"Eligible topic partition leaders are not available.",
		"84":"Leader election not needed for topic partition.",
		"85":"No partition reassignment is in progress.",
		"86":"Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
		"87":"This record has failed the validation on broker and hence will be rejected.",
		"88":"There are unstable offsets that need to be cleared."
		};