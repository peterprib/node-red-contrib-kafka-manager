const logger = new (require('node-red-contrib-logger'))('Kafka Consumer Group')
logger.sendInfo('Copyright 2020 Jaroslav Peter Prib')
const setupHttpAdmin = require('./setupHttpAdmin.js')
const State = require('./state.js')
const commonConsumerHostState = require('./commonConsumerHostState.js')
const commonConsumerUpAction = require('./commonConsumerUpAction.js')

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
    onRebalance: (isAlreadyMember, done) => { done() } // or null
  }
  if (_this.brokerNode.TLSOptions) _this.options.sslOptions = _this.brokerNode.TLSOptions
  return options
}

module.exports = function (RED) {
  function KafkaConsumerGroupNode (n) {
    RED.nodes.createNode(this, n)
    this.status({ fill: 'yellow', shape: 'ring', text: 'Initialising' })
    this.state = new State(this)
    try {
      const node = Object.assign(this, n, { connected: false, paused: false, timedout: false })
      this.state
        .setUpAction((next,error)=> {
          logger.active&&logger.send({ label: 'upAction',node:{id:node.id,name:node.name}})
          node.status({ fill: 'yellow', shape: 'ring', text: 'Connecting' })
          node.messageCount = 0
          node.consumer = new node.brokerNode.Kafka.ConsumerGroup(getOptions(node), node.topics)
          commonConsumerUpAction(node,next,error,logger)
        })
        .setDownAction(next => {
          logger.active&&logger.send({ label: 'downAction close', node: node.id, name: node.name })
          node.status({ fill: 'red', shape: 'ring', text: 'Closing' })
          node.consumer.close(false, () => {
            node.status({ fill: 'red', shape: 'ring', text: 'Closed' })
            node.down(nextDown=>{
              logger.active&&logger.send({ label: 'downAction down', node: node.id, name: node.name })
              next()
              nextDown()
            })
          })
        })
      node.brokerNode = RED.nodes.getNode(node.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      commonConsumerHostState(node,logger)
      node.client=node.brokerNode.client

    } catch (ex) {
      this.status({ fill: 'red', shape: 'ring', text: ex.toString() })
      logger.sendErrorAndStackDump(ex.message, ex)
      this.error(ex.toString())
    }
  }
  RED.nodes.registerType(logger.label, KafkaConsumerGroupNode)
  setupHttpAdmin(RED, logger.label, {
    close: (RED, node, done) => {
      node.testUp()
      node.setDown(done)
    },
    open: (RED, node, done) => {
      node.testDown()
      node.setUp(done)
    },
    commit: (RED, node, done) => {
      node.testUp()
      node.commit(() => done(), (ex) => done(null, 'close error: ' + ex.message))
    },
    pause: (RED, node, done) => {
      node.testUp()
      node.pause(done)
    },
    resume: (RED, node, done) => {
      node.testUp()
      node.resume(done)
    },
    refresh: (RED, node, done) => {
      node.testUp()
      const error = node.brokerNode.metadataRefresh()
      done(null, error)
    },
    status: (RED, node, done) => done({
      node: node.getState(),
      client: node.client.getState(),
      host: node.brokerNode.hostState.getState()
    })
  })
}
