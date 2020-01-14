const nodeName="Kafka Consumer";
const Logger = require("logger");
const logger = new Logger(nodeName);
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");

let kafka;

function sendMsg (node, message) {
  if(logger.active) logger.send({
    label: 'sendMsg',
    node: node.id,
    message: message
  })
  node.send({
    topic: message.topic || node.topic,
    payload: message.value,
    _kafka: {
      offset: message.offset,
      partition: message.partition,
      highWaterOffset: message.highWaterOffset,
      key: message.key
    }
  })
}

function connect (node) {
  if(logger.active) logger.send({
    label: 'connect',
    node: node.id
  })
  node.client = node.brokerNode.getKafkaClient()
  node.consumer = new kafka[(node.connectionType || 'Consumer')](node.client, node.topics, {
    groupId: node.groupId || 'kafka-node-group',
    autoCommit: (node.autoCommit || 'true') === 'true',
    autoCommitIntervalMs: node.autoCommitIntervalMs || 5000,
    fetchMaxWaitMs: node.fetchMaxWaitMs || 100,
    fetchMinBytes: node.fetchMinBytes || 1,
    fetchMaxBytes: node.fetchMaxBytes || 1024 * 1024,
    fromOffset: node.fromOffset || 0,
    encoding: node.encoding || 'utf8',
    keyEncoding: node.keyEncoding || 'utf8'
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
      if (message.value == null) return //  seems to send an empty on connect in no messages waiting
    }
    if (node.timedout) {
      node.timedout = false
      node.status({
        fill: 'green',
        shape: 'ring',
        text: 'Ready with ' + node.brokerNode.name
      })
    }
    if (Array.isArray(message)) {
      message.forEach((r) => sendMsg(node, r))
    } else {
      sendMsg(node, message)
    }
  })

  node.consumer.on('error', function (e) {
    if(logger.active) logger.send({
      label: 'consumer.on.error',
      node: node.id,
      error: e
    })
    if (e.message.startsWith('Request timed out')) {
      node.status({
        fill: 'yellow',
        shape: 'ring',
        text: e.message
      })
      node.log('on error ' + e.message)
      node.timedout = true
      return
    }
    node.error('on error ' + e.message)

    const err = node.brokerNode.getRevisedMessage(e.message)
    node.status({
      fill: 'red',
      shape: 'ring',
      text: err
    })
  })
  node.consumer.on('offsetOutOfRange', function (e) {
    if(logger.active) logger.send({
      label: 'consumer.on.offsetOutOfRange',
      node: node.id,
      error: e
    })
    node.error('on offsetOutOfRange ' + e)
    node.status({
      fill: 'red',
      shape: 'ring',
      text: e.message + ' (PAUSED)'
    })
    node.consumer.pause()
  })
}
module.exports = function (RED) {
  function KafkaConsumerNode (n) {
    RED.nodes.createNode(this, n)
    var node = Object.assign(this, n, {
      connected: false,
      paused: false,
      timedout: false
    })
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
          connect(node)
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
        if(logger.active) logger.send({
          label: 'close',
          node: node.id
        })
        node.status({
          fill: 'red',
          shape: 'ring',
          text: 'closed'
        })
        node.consumer.close(false, () => {
          node.log('closed')
        })
        done()
      })
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
  RED.nodes.registerType('Kafka Consumer', KafkaConsumerNode)
  RED.httpAdmin.get('/KafkaConsumer/:id/:action/', RED.auth.needsPermission('KafkaConsumer.write'), function (req, res) {
    var node = RED.nodes.getNode(req.params.id)
    if (node && node.type === 'Kafka Consumer') {
      try {
        switch (req.params.action) {
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
