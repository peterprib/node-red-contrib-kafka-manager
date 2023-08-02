const logger = new (require('node-red-contrib-logger'))('Kafka Consumer Group')
logger.sendInfo('Copyright 2020 Jaroslav Peter Prib')
const setupHttpAdmin = require('./setupHttpAdmin.js')
const State = require('./state.js')

function getOptions (_this = this) {
  const options = {
    kafkaHost: _this.brokerNode.kafkaHost, // connect directly to kafka broker (instantiates a KafkaClient)
    // batch: undefined, // put client batch settings if you need them
    // ssl: true, // optional (defaults to false) or tls options hash
    groupId: _this.groupId,
    sessionTimeout: _this.sessionTimeout, // default: 5000,
    protocol: _this.protocol, // default: ['roundrobin'],
    encoding: _this.encoding, // default: utf8
    fromOffset: _this.fromOffset, // default: 'latest'
    commitOffsetsOnFirstJoin: _this.commitOffsetsOnFirstJoin === 'true',
    outOfRangeOffset: _this.outOfRangeOffset, // default: 'earliest'
    onRebalance: (isAlreadyMember, callback) => { callback() } // or null
  }
  if (_this.brokerNode.TLSOptions) _this.options.sslOptions = _this.brokerNode.TLSOptions
  return options
}
/*
function clientReady (_this = this) {
  _this.messageCount = 0
  _this.ready = false
  _this.options = getOptions(_this)
  const kafka = _this.brokerNode.getKafkaDriver()
  _this.consumer = new kafka.ConsumerGroup(_this.options, _this.topics)
  _this.consumer.on('connect', function () {
    if (logger.active) logger.send({ label: 'consumer.on.connect', node: _this.id,	name: _this.name })
    _this.connected = true
  })
  _this.consumer.on('message', (message) => {
    if (logger.active) logger.send({ label: 'consumer.on.message', node: _this.id,	name: _this.name, message: message })
    if (++_this.messageCount == 1) {
      _this.status({	fill: 'green',	shape: 'ring', text: 'Processing Messages' })
      if (message.value == null) return //	seems to send an empty on connect in no messages waiting
    }
    if (_this.timedout) {
      _this.timedout = false
      _this.status({	fill: 'green',	shape: 'ring', text: 'Processing Messages' })
    }
    _this.brokerNode.sendMsg(node, message)
  })

  _this.consumer.on('rebalancing', () => {
    logger.info({ label: 'consumerGroup.on.rebalancing', node: _this.id,	name: _this.name })
  })
  _this.consumer.on('error', (e) => {
    if (logger.active) logger.send({ label: 'consumer.on.error', node: _this.id,	name: _this.name, error: e })
    const err = e.message ? e.message : e.toString()
    if (err.startsWith('Request timed out')) {
      _this.status({	fill: 'yellow',	shape: 'ring', text: e.message })
      node.timedout = true
      return
    }
    _this.status({	fill: 'red',	shape: 'ring', text: _this.brokerNode.getRevisedMessage(err) })
  })
  _this.consumer.on('offsetOutOfRange', (err) => {
    if (logger.active) logger.send({ label: 'consumer.on.offsetOutOfRange', node: node.id,	name: _this.name, error: err })
    _this.consumer.pause()
    _this.status({	fill: 'red',	shape: 'ring', text: 'offsetOutOfRange ' + err.message + ' (PAUSED)' })
  })
  if (this.paused) {
    this.log('state changed to up and in paused state')
    _this.status({	fill: 'red',	shape: 'ring', text: 'Paused' })
    return
  }
  this.log('state changed to up, resume issued')
  this.resume()
}

*/
module.exports = function (RED) {
  function KafkaConsumerGroupNode (n) {
    RED.nodes.createNode(this, n)
    this.status({	fill: 'yellow',	shape: 'ring', text: 'Initialising' })
    this.state = new State(this)
    try {
      const node = Object.assign(this, n, { connected: false, paused: false, timedout: false })
      this.state
      .onUp(()=>this.status({	fill: 'green',	shape: 'ring', text: 'active' }))
      .onDown(()=>node.status({	fill: 'red',	shape: 'ring', text: 'down' }))
      .setUpAction(()=>{
        node.status({	fill: 'yellow',	shape: 'ring', text: 'Connecting' })
        node.messageCount = 0
        const kafka = node.brokerNode.getKafkaDriver()
        node.consumer = new kafka.ConsumerGroup(getOptions(node),node.topics)
        node.consumer.on('connect', function () {
          if (logger.active) logger.send({ label: 'consumerGroup on.connect', node:node.id,	name:node.name })
          if(node.paused) {
            node.log('state changed to up and in paused state')
            node.status({	fill: 'red',	shape: 'ring', text: 'Paused' })
          } else{
            node.log('state changed to up, resume issued')
            node.resume()
          }
          node.available()
        })
        node.consumer.on('message', (message) => {
          if (logger.active) logger.send({ label: 'consumerGroup on.message', node:node.id,name:node.name, message: message })
          if (++node.messageCount == 1 || node.timedout) {
            node.timedout = false
            node.status({	fill: 'green',	shape: 'ring', text: 'Processing Messages' })
            if (message.value == null) return //	seems to send an empty on connect in no messages waiting
          }
          node.brokerNode.sendMsg(node, message)
        })
        node.consumer.on('rebalancing', () => {
          logger.info({ label: 'consumerGroup on.rebalancing', node:node.id,name:node.name })
        })
        node.consumer.on('error', (e) => {
          if (logger.active) logger.send({ label: 'consumerGroup on.error', node:node.id,name:node.name, error: e })
          const err = e.message ? e.message : e.toString()
          if (err.startsWith('Request timed out')) {
            node.status({	fill: 'yellow',	shape: 'ring', text: e.message })
            node.timedout = true
            return
          }
          node.status({fill:'red',shape:'ring',text:node.brokerNode.getRevisedMessage(err)})
          node.down()
        })
        node.consumer.on('offsetOutOfRange', (err) => {
          if(logger.active) logger.send({ label: 'consumer.on.offsetOutOfRange', node:node.id,	name:node.name, error: err })
          node.consumer.pause()
          node.status({	fill: 'red',	shape: 'ring', text: 'offsetOutOfRange ' + err.message + ' (PAUSED)' })
        })
      })
      .setDownAction(()=>{
        if (logger.active) logger.send({ label: 'close', node:node.id,	name:node.name })
        node.status({	fill: 'red',	shape: 'ring', text: 'Closing' })
        node.consumer.close(false, () => {
          node.status({	fill: 'red',	shape: 'ring', text: 'Closed' })
          node.down()
        })
      })
      node.brokerNode = RED.nodes.getNode(node.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.brokerNode.onUp(node.setUp.bind(node))
      node.on('close', function (removed, done) {
        if (logger.active) logger.send({ label: 'close', node: node.id,	name: node.name })
        node.setDown(done)
      })
      node.pause = (callback) => {
        if (logger.active) logger.send({ label: 'pause', node: node.id,	name: node.name })
        node.paused = true
        node.consumer.pause()
        node.status({	fill: 'red',	shape: 'ring', text: 'Paused' })
        callback && callback()
      }
      node.resume = (callback) => {
        if (logger.active) logger.send({ label: 'resume', node: node.id,	name: node.name })
        node.resumed = true
        node.consumer.resume()
        node.status({	fill: 'green',	shape: 'ring', text: 'Ready' })
        callback && callback()
      }
      node.commit = (callBack) => {
        node.consumer.commit((err, data) => {
          if (logger.active) logger.send({ label: 'commit', node: node.id,	name: node.name, error: err, data: data })
          callback && callback(data, err)
        })
      }
    } catch (ex) {
      this.status({ fill: 'red', shape: 'ring', text: ex.toString() })
      logger.sendErrorAndStackDump(ex.message, ex)
      this.error(ex.toString())
    }
  }
  RED.nodes.registerType(logger.label, KafkaConsumerGroupNode)
  setupHttpAdmin(RED, logger.label, {
    close: (RED, node, callback) => {
      node.testUp()
      node.setDown(callback)
    },
    open: (RED, node, callback) => {
      node.testDown()
      node.setUp(callback)
    },
    commit: (RED, node, callback) => {
      node.testUp()
      node.commit(() => callback(), (ex) => callback(null, 'close error: ' + ex.message))
    },
    pause: (RED, node, callback) => {
      node.testUp()
      node.pause(callback)
    },
    resume: (RED, node, callback) => {
      node.testUp()
      node.resume(callback)
    },
    refresh: (RED, node, callback) => {
      node.testUp()
      const error = node.brokerNode.metadataRefresh()
      callback(null, error)
    }
  })
}
