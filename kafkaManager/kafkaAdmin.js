/* eslint-disable no-prototype-builtins */
const logger = new (require('node-red-contrib-logger'))('Kafka Admin')
logger.sendInfo('Copyright 2020 Jaroslav Peter Prib')
const setupHttpAdmin = require('./setupHttpAdmin.js')
const State = require('./state.js')
const formatError = require('./formatError.js')
function msgProcess (msg, errObject, data) {
  const _this = this
  if (logger.active) logger.send({ label: 'msgProcess', error: errObject, data: data })
  if (errObject) {
    const err = typeof errObject !== 'string' ? errObject.toString() : errObject.message.toString()
    if (err.startsWith('Broker not available') || err.startsWith('Request timed out')) {
      this.warn('Broker not available, queue message and retry connection as ' + err)
      this.whenUp(this.processInputWithOnDisgard, msg)
      return
    }
    this.error(msg.topic + ' ' + err)
    msg.error = err
    this.send([null, msg])
    return
  }
  if (['createAcls', 'createTopics', 'deleteAcls', 'deleteTopics', 'electPreferredLeaders'].includes(msg.topic)) {
    data.forEach((c, i, a) => {
      const t = msg.payload.find((cp) => cp.topic === c.topic)
      if (logger.active) logger.send({ label: 'msgProcess multi response', topic: c, data: t })
      if (c.hasOwnProperty('error')) {
        if (logger.active) logger.send({ label: 'msgProcess multi response', data: { topic: msg.topic, error: formatError(c.error), payload: [t] } })
        _this.send([null, {
          topic: msg.topic,
          error: formatError(c.error),
          payload: [t]
        }])
        return
      }
      if (logger.active) logger.send({ label: 'msgProcess multi response ok', data: { topic: msg.topic, payload: [c] } })
      _this.send({
        topic: msg.topic,
        payload: [t]
      })
    })
  } else {
    msg.payload = data
    this.send(msg)
  }
}
const processInputNoArg = ['listConsumerGroups', 'listGroups', 'listTopics']
const processInputPayloadArg = [
  'alterConfigs', 'alterReplicaLogDirs', 'createAcls', '', 'createDelegationToken',
  'createPartitions', 'createTopics', 'deleteAcls', 'deleteConsumerGroups', 'deleteRecords',
  'deleteTopics', 'describeAcls', 'describeConsumerGroups',
  'describeGroups', 'describeLogDirs', 'describeTopics', 'electPreferredLeaders',
  'expireDelegationToken', 'incrementalAlterConfigs', 'listConsumerGroupOffsets',
  'renewDelegationToken'
]

function processInput (msg, done = (err, data) => this.msgProcess(msg, err, data), onError) {
  if (logger.active) logger.send({ label: 'processInput', msg })
  try {
    if (this.client.connection == null) throw Error('no connection')
    const action = msg.topic
    if (processInputNoArg.includes(action)) {
      if (logger.active) logger.send({ label: 'processInput processInputNoArg', msg })
      this.client.connection[action](done)
      return
    }
    if (processInputPayloadArg.includes(action)) {
      if (logger.active) logger.send({ label: 'processInput processInputPayloadArg', msg })
      this.connection[action](msg.payload, done)
      return
    }
    let resource = {}
    let payload = {}
    switch (action) {
      case 'describeConfigs':
        // msg.payload={type:'topic',name:'a-topic'}
        resource = {
          resourceType: this.connection.RESOURCE_TYPES[msg.payload.type || 'topic'], // 'broker' or 'topic'
          resourceName: msg.payload.name,
          configNames: [] // specific config names, or empty array to return all,
        }
        payload = {
          resources: [resource],
          includeSynonyms: false // requires kafka 2.0+
        }
        this.connection.describeConfigs(payload, done)
        break
      default:
        throw Error('invalid message topic')
    }
  } catch (ex) {
    if (logger.active) logger.send({ label: 'processInput catch', error: ex.message, msg: msg, connection: Object.keys(this.connection || {}), stack: ex.stack })
    if (onError) {
      onError(ex)
      return
    }
    msg.error = ex.message
    this.send([null, msg])
  }
}
function onDisgard (_error) {
  this.status({ fill: 'red', shape: 'ring', text: 'Closed, disgarded messages' })
}
module.exports = function (RED) {
  function KafkaAdminNode (n) {
    RED.nodes.createNode(this, n)
    this.state = new State(this)
    try {
      const node = Object.assign(this, n, {
        msgProcess: msgProcess.bind(this),
        processInput: processInput.bind(this),
        processInputWithOnDisgard: { call: processInput.bind(this), onDisgard: onDisgard.bind(this) }
      })
      node.state.setUpOnUpQDepth(0)
        .setUpAction((up) => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'Connecting' })
          node.client.whenUp(() => {
            logger.info({ label: 'client.on.ready', node: node.id, name: node.name })
            node.connection = new node.brokerNode.Kafka.Admin(node.client.connection)
            up()
          })
        }).setDownAction((down) => {
          node.status({ fill: 'red', shape: 'ring', text: 'closing' })
          down()
        }).onDown(() => {
          if (node.client.isNotAvailable()) return
          node.client.setDown()
        }).onUp(() => {
          node.status({ fill: 'green', shape: 'ring', text: 'connectioned' })
        }).onError((error) => {
          node.error(error)
        })
      node.status({ fill: 'yellow', shape: 'ring', text: 'Initialising' })
      node.brokerNode = RED.nodes.getNode(node.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.status({ fill: 'red', shape: 'ring', text: 'broker down' })
      node.brokerNode.hostState
        .onUp(() => {
          node.status({ fill: 'green', shape: 'ring', text: 'ready auto connect' })
        })
        .onDown(() => {
          if (node.client.isAvailable()) node.client.forceDown()
          node.status({ fill: 'red', shape: 'ring', text: 'broker down' })
        })
      node.client = node.brokerNode.getClient()
      node.client.setIdleTime(10).setUpOnUpQDepth(0)
        .onDown(() => {
          node.status({ fill: 'green', shape: 'ring', text: 'ready auto connect' })
          if (node.isAvailable()) node.forceDown()
        }).onUp(() => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'client connected' })
        })
      node.on('input', function (msg) {
        node.whenUp(node.processInput, msg)
      })
      node.on('close', function (removed, done) {
        node.setDown(done)
      })
    } catch (ex) {
      this.status({ fill: 'red', shape: 'ring', text: ex.toString() })
      logger.sendErrorAndStackDump(ex.message, ex)
      this.error(ex.toString())
    }
  }
  RED.nodes.registerType(logger.label, KafkaAdminNode)
  setupHttpAdmin(RED, logger.label, {
    listGroups: (RED, node, callback) => {
      node.whenUp(node.processInput, { topic: 'listGroups' },
        (err, data) => callback(data, err),
        ex => callback(null, ex.message))
    },
    listTopics: (RED, node, callback) => {
      node.whenUp(node.processInput, { topic: 'listTopics' }, (err, data) => callback(data, err), ex => callback(null, ex.message))
    },
    close: (RED, node, done) => {
      node.testUp()
      node.setDown(done)
    },
    connect: (RED, node, done) => {
      node.testDown()
      if (node.isNotAvailable()) {
        node.setUp(done)
      } else { node.setUp(done) }
    },
    status: (RED, node, done) => done({
      node: node.getState(),
      client: node.client.getState(),
      host: node.brokerNode.hostState.getState()
    })
  })
}
