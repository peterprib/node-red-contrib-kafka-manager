const logger = new (require('node-red-contrib-logger'))('Kafka Consumer')
logger.sendInfo('Copyright 2020 Jaroslav Peter Prib')
const Logger = require('node-red-contrib-logger');
const getDataType = require('./getDataType.js')
//const zlib = require('node:zlib');

let kafka;
/*
function sendMessage(node, message) {
  if(node.convertToJson) try{
    message.value=JSON.parse(message.value);
  } catch(ex){
    message.error="JSON parse error: "+ex.message;
  }
  node.brokerNode.sendMsg(node, message);
}
*/
function connect(node) {
  if (logger.active) logger.send({ label: 'connect', node: node.id })
  if (node.consumer) throw Error('already open')
  if (node.opening) throw Error('already opening')
  node.opening = true
  node.status({	fill: 'yellow', shape: 'ring', text: 'Open and wait ' + node.brokerNode.name	})
  node.client = node.brokerNode.getKafkaClient()
  logger.info({ label: 'connecting', activeTopics: node.activeTopics, wildcard: this.regex, topics: node.topics })
  node.consumer = new kafka[(node.connectionType || 'Consumer')](node.client, node.activeTopics, {
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
    node.brokerNode.sendMsg(node, message);
  })

  node.consumer.on('error', function (e) {
    if (logger.active) logger.send({ label: 'consumer.on.error', node: node.id,	error: e })
    const err = e.message ? e.message : e.toString()
    if (err.startsWith('Request timed out')) {
      node.status({ fill: 'yellow',	shape: 'ring', text: err	})
      node.log('on error ' + err)
      node.timedout = true
      return
    }
    node.error('on error ' + err)
    node.status({ fill: 'red', shape: 'ring', text: node.brokerNode.getRevisedMessage(err) })
  })
  node.consumer.on('offsetOutOfRange', function (ex) {
    if (logger.active) logger.send({ label: 'consumer.on.offsetOutOfRange', node: node.id,	error: ex })
    node.error('on offsetOutOfRange ' + ex)
    node.status({	fill: 'red',	shape: 'ring', text: ex.message + ' (PAUSED)'	})
    node.consumer.pause()
  })
}
function onChangeMetadata (change) {
  if (logger.active) logger.send({ label: 'onChangeMetadata', node: this.id,	change: change, filters: this.filters.length })
  const node = this
  const removeTopics = change.remove
  const addTopics = change.add.filter(cell => node.filters.find(regex => regex.test(cell.topic)))
  if (logger.active) logger.send({ label: 'onChangeMetadata actions', node: this.id,	add: addTopics, remove: removeTopics })
  if (!node.consumer) {
    if (addTopics.length + addTopics.length == 0) return
		 logger.warn({ label: 'onChangeMetadata', id: node.id, warn: 'consumer down', topics: node.activeTopics, remove: removeTopics, add: addTopics })
    this.activeTopics = this.activeTopics.filter(p => !removeTopics.find(c => p.topic == c.topic && c.partition == p.partition))
    this.activeTopics.push(...addTopics)
    return
  }
  if (addTopics.length > 0) {
		 logger.warn({ label: 'onChangeMetadata add topics', id: node.id, topics: node.activeTopics, add: addTopics })
    node.addTopics(addTopics, undefined, err => {
      if (err) {
        node.error('auto add topics for ' + JSON.stringify(addTopics) + ' error:' + err)
      }
    })
  } else if (removeTopics.length > 0) { // else to only allow one at a time
		 logger.warn({ label: 'onChangeMetadata remove topics', id: node.id, topics: node.activeTopics, remove: removeTopics })
    node.removeTopics(removeTopics, undefined, err => {
      if (err) {
        node.error('auto remove topics for ' + JSON.stringify(addTopics) + ' error:' + err)
      }
    })
  }
}
module.exports = function (RED) {
  function KafkaConsumerNode (n) {
    RED.nodes.createNode(this, n)
    const node = Object.assign(this,n)
    node.autoCommitBoolean = (node.autoCommit || 'true') === 'true'
    node.brokerNode = RED.nodes.getNode(node.broker)
    if (node.regex) {
      node.activeTopics = []
      node.filters = node.topics.map(t => new RegExp(t.topic))
      logger.info({ label: 'regex', node: node.id, topics: node.topics })
      node.brokerNode.onChangeMetadata(onChangeMetadata.bind(node))
      node.status({	fill: 'yellow', shape: 'ring',	text: 'Initialising wildcard topics' })
    } else {
      if (!node.topics) node.activeTopics = [{ topic: node.topic, partition: 0 }] // legacy can be removed in future
      node.activeTopics = node.topics
      node.status({	fill: 'yellow', shape: 'ring',	text: 'Initialising' })
    }
    try {
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.brokerNode.setConsumerProperties(node);
      if (!kafka) kafka = node.brokerNode.getKafkaDriver()
      node.brokerNode.onStateUp.push({
        node: node,
        callback: function () {
          if (logger.active) logger.send({ label: 'brokerNode.stateUp', node: node.id })
          connect(node)
        }
      }) // needed due to bug in kafka driver
      node.brokerNode.stateUp.push({
        node: node,
        callback: function () {
          if (logger.active) logger.send({ label: 'brokerNode.stateUp', node: node.id })
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
        if (logger.active) logger.send({ label: 'on.close', node: node.id })
        node.status({	fill: 'red', shape: 'ring', text: 'closed'	})
        //				if(node.releaseStale) clearInterval(node.releaseStale);
        node.consumer.close(false, () => {
          if (logger.active) logger.send({ label: 'on.close consumer.close', node: node.id })
          delete node.consumer
          node.log('closed')
          done()
        })
      })
      //			if(!node.autoCommitBoolean){
      //				node.releaseStale = setInterval(function(node) {releaseStale(node)}, 1000*60,node);
      //			}
      node.close = (okCallback, errCallback) => {
        node.brokerNode.closeNode(node, okCallback, errCallback)
      }
      node.open = (okCallback, errCallback) => {
        if (logger.active) logger.send({ label: 'open', node: node.id	})
        try {
          if (node.brokerNode.available !== true) throw Error('broker ' + node.brokerNode.name + ' available: ' + node.brokerNode.available)
          connect(node)
        } catch (ex) {
          if (errCallback) errCallback(ex)
          return
        }
        if (okCallback) okCallback()
      }
      node.pause = () => {
        if (logger.active) logger.send({ label: 'pause', node: node.id })
        node.paused = true
        node.consumer.pause()
        node.status({	fill: 'red',	shape: 'ring',	text: 'paused'	})
      }
      node.resume = () => {
        if (logger.active) logger.send({ label: 'resume', node: node.id	})
        node.resumed = true
        node.consumer.resume()
        node.status({	fill: 'green',	shape: 'ring', text: 'Ready with ' + node.brokerNode.name })
      }
      node.addTopics = (topics, fromOffset, callBack) => {
        if (logger.active) logger.send({ label: 'consumer.addTopics', node: node.id,	topics: topics, fromOffset: fromOffset })
        node.consumer.addTopics(topics,
          (err, added) => {
            if (logger.active) logger.send({ label: 'consumer.addTopics callback', node: node.id,	topics: topics, fromOffset: fromOffset, added: added,	error: err	})
            if (err) node.error('add topics to consumer failed error:' + err)
            else node.activeTopics.push(...topics)
            callBack && callBack(err)
          },
          fromOffset
        )
      }
      node.removeTopics = (topics, callback) => {
        if (logger.active) logger.send({ label: 'consumer.removeTopics', node: node.id,	topics: topics })
        node.consumer.removeTopics(topics, (err, removed) => {
          if (logger.active) logger.send({ label: 'consumer.removeTopics callback', node: node.id,	topics: topics, removed: removed, error: err })
          if (err) node.error('remove topics from consumer failed error:' + err)
          else node.activeTopics = node.activeTopics.filter(p => !topics.find(c => p.topic == c.topic && c.partition == p.partition))
          callback && callback(err)
        })
      }
      node.commit = () => {
        node.consumer.commit((err, data) => {
          if (logger.active) logger.send({ label: 'commit',	node: node.id, error: err, data: data })
        })
      }
      node.setOffset = (topic, partition, offset) => node.consumer.setOffset(topic, partition, offset)
      node.pauseTopics = (topics) => node.consumer.pauseTopics(topics)
      node.resumeTopics = (topics) => node.consumer.resumeTopics(topics)
    } catch (ex) {
      logger.sendErrorAndStackDump("",ex);
      node.error(ex.toString())
      node.status({ fill: 'red', shape: 'ring', text: ex.toString() })
    }
  }
  RED.nodes.registerType(logger.label, KafkaConsumerNode)
  RED.httpAdmin.post('/KafkaConsumer/:id/:action', RED.auth.needsPermission('KafkaConsumer.write'), function (req, res) {
    if (logger.active) logger.send({ label: 'httpAdmin.post', parms: req.params, data: req.body })
    const node = RED.nodes.getNode(req.params.id)
    try {
      if (node == null) throw Error('node not found')
      if (node.type !== logger.label) throw Error('node found but wrong type')
      const topics = req.body.topics
      switch (req.params.action) {
        case 'addTopic':
        case 'addTopics':
          if (node.regex) return res.status(500).send('wildcard topics')
          node.addTopics(topics, undefined, err => {
            if (err) return res.status(500).send(err)
            return res.sendStatus(200)
          })
          return
        case 'removeTopic':
        case 'removeTopics':
          if (node.regex) return res.status(500).send('wildcard topics')
          node.removeTopics(topics, undefined, err => {
            if (err) return res.status(500).send(err)
            return res.sendStatus(200)
          })
          break
        default:
          res.status(404).send('request to ' + req.params.action + ' failed for id:' + req.params.id)
          return
      }
      node.warn('Request to ' + req.params.action)
      res.sendStatus(200)
    } catch (err) {
      const reason1 = 'Internal Server Error, id: ' + req.params.id + ' action: ' + req.params.action + ' failed ' + err.toString()
      node && node.error(reason1)
      res.status(500).send(reason1)
    }
  })
  RED.httpAdmin.get('/KafkaConsumer/:id/:action', RED.auth.needsPermission('KafkaConsumer.write'), function (req, res) {
    if (logger.active) logger.send({ label: 'httpAdmin.get', parms: req.params })
    const node = RED.nodes.getNode(req.params.id)
    try {
      if (node == null) throw Error('flow node not found')
      if (node.type !== logger.label) throw Error('flow node found but wrong type')
      switch (req.params.action) {
        case 'activeTopics':
          res.status(200).json(node.activeTopics || [])
          return
        case 'allTopics':
          const topics = node.brokerNode.getTopicsPartitions()
          if (topics == null) throw Error('getTopicsPartitions returned null')
          res.status(200).json(topics)
          return
        case 'close':
          node.close(() => res.sendStatus(200), (ex) => { const err = 'close error: ' + ex.message; node.warn(err); res.status(500).send(err) })
          return
        case 'open':
          node.open(() => res.sendStatus(200), (ex) => { const err = 'open error: ' + ex.message; node.warn(err); res.status(500).send(err) })
          return
        case 'pause':
          node.pause()
          break
        case 'refresh':
          const error = node.brokerNode.metadataRefresh()
          if (error) throw Error(error)
          break
        case 'resume':
          node.resume()
          break
        default:
          res.status(404).send('request to ' + req.params.action + ' failed for id:' + req.params.id)
          return
      }
      node.warn('Request to ' + req.params.action)
      res.sendStatus(200)
    } catch (ex) {
      const reason1 = 'Internal Server Error, id: ' + req.params.id + ' action: ' + req.params.action + ' failed ' + ex.toString()
      node && node.error(reason1)
      logger.error({ label: 'httpAdmin.get', error: ex.message, stack: ex.stack })
      res.status(500).send(reason1)
    }
  })
}
