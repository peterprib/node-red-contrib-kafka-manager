/* eslint-disable no-prototype-builtins */
const logger = new (require('node-red-contrib-logger'))('Kafka Producer')
logger.sendInfo('Copyright 2022 Jaroslav Peter Prib')
const setupHttpAdmin = require('./setupHttpAdmin.js')
const evalInFunction = require('./evalInFunction.js')
// const getDataType = require('./getDataType.js')
const State = require('./state.js')
const nodeStatus = require('./nodeStatus.js')
const zlib = require('node:zlib')
const compressionTool = require('compressiontool')
const noderedBase=require("node-red-contrib-noderedbase");

function showStatus (statusText, statusfill) {
  this.statusText = statusText
  this.statusfill = statusfill
  this.status({ fill: this.statusfill, shape: 'ring', text: this.statusText })
}
/*
const messageAttributes = (msg) => {
  if (msg == null) return '***Message not define***'
  return {
    messageDataType: getDataType(msg.messages[0]),
    topic: msg.topic,
    key: msg.key,
    partition: msg.partition,
    attributes: msg.attributes
  }
}
*/
function sendKafka (msg, done) {
  /*
  //node.KeyedMessage = kafka.KeyedMessage
  //km = new KeyedMessage('key', 'message'),
  //payloads = [
  //  { topic: 'topic1', messages: 'hi', partition: 0 },
  //  { topic: 'topic2', messages: ['hello', 'world', km] }
  //];

{ topic: 'topicName',
  messages: ['message body'], // multi messages should be a array, single message can be just a string or a KeyedMessage instance
  key: 'theKey', // string or buffer, only needed when using keyed partitioner
  partition: 0, // default 0
  attributes: 2, // default: 0
  timestamp: Date.now() // <-- defaults to Date.now() (only available with kafka v0.10+)
}
attributes: 0: No compression, 1: Compress using GZip, 2: Compress using snappy
*/

  const _this = this
  logger.active&&logger.send({ label: 'sendKafka', node: _this.id, msg: sendMsgToString, doneProvide: done != null })
  _this.producer.send([msg], (err, data) => {
    if (err) {
      logger.error({ label: 'send', node: _this.id, error: err })
      const errmsg = (typeof err === 'string' ? err : err.message)
      if (errmsg.startsWith('InvalidTopic')) {
        _this.status({ fill: 'yellow', shape: 'ring', text: 'Invalid topic ' + msg.topic })
      } else if (errmsg.startsWith('Broker not available')) {
        try {
          logger.warn("broker down, setting state down")
          _this.brokerNode.setDown()
        } catch (ex) {
          _this.error(ex.message)
        }
        done && done(err)
        return
      } else if (errmsg.startsWith('Could not find the leader')) {
        _this.brokerNode.client.connection.refreshMetadata([msg.topic], (error) => { // bug workaround
          if (error) {
            _this.error('producer.send client.refreshMetadata ' + error)
            _this.showStatus('leader reset issue', 'red')
            done && done(err)
          } else {
            _this.log('refreshMetadata issued')
            _this.producer.send([msg], (err, data) => {
              if (err) {
                _this.error('retry send error ' + err)
                _this.sendDeadletter(msg, done)
              } else {
                _this.successes++
              }
              done && done(err)
            })
          }
        })
        /*
        _this.log('issuing loadMetadataForTopics as may be issue with caching')
        _this.brokerNode.client.connection.loadMetadataForTopics([msg.topic], (error, metadata) => {
          if(error){
            _this.error('producer.send client.loadMetadataForTopics '+error)
            _this.showStatus('leader reset issue', 'red')
            done && done(err)
          } else {
            _this.producer.send([msg], (err, data) => {
              if(err) {
                _this.error('retry send error '+err)
                _this.sendDeadletter(msg,done)
              } else {
                _this.successes++
              }
              done && done(err)
            })
          }
        })
*/
        return
      }
      _this.sendDeadletter(msg, done)
    } else {
      _this.successes++
    }
    done && done(err)
  })
}

function sendDeadletterError (msg, err, _this = this) {
  _this.error('dead letter failed turned off')
  _this.sendDeadletter = sendDeadletterNotOK.bind(_this)
  _this.sendDeadletter(msg)
}
function sendDeadletterNewOK (msg, _this = this) {
  try {
    const message = JSON.stringify(msg)
    zlib.gzip(message, (err, buffer) => {
      if (err) {
        logger.error({ label: 'deadletter gzip fail', error: err, deadletterTopic: _this.deadletterTopic })
        this.sendDeadletterError.apply(_this, [msg, err])
        return
      }
      const deadletterMsg = {
        topic: _this.topicDeadletter || 'deadletter',
        messages: [buffer],
        attributes: 0
      }
      _this.producer.send([deadletterMsg],
        (err, data) => {
          if (err) {
            logger.error({ label: 'deadletter.producer.send', error: err, deadletterTopic: _this.deadletterTopic })
            _this.sendDeadletterError.apppy(_this, [msg, err])
          } else {
            _this.deadletters++
          }
        })
    })
  } catch (ex) {
    logger.error({ label: 'deadletter.producer.send', error: ex.message, deadletterTopic: _this.deadletterTopic })
    _this.sendDeadletterError.apppy(_this, [msg, ex.message])
  }
}

function sendDeadletterNotOK (msg) {
  this.errorDiscarded++
  this.error('discarded', msg)
}
function sendMsgToString (msg) {
  return { topic: msg.topic, key: msg.key, partition: msg.partition, attributes: msg.attributes }.toString()
}
function getTopicNodeDefault (msg) {
  const msgTopic = this.topicSlash2dot && msg.topic ? msg.topic.replace('/', '.') : msg.topic
  return msgTopic || this.topic || ''
}
function getTopicNode () { return this.topic || '' }
function convert2KafkaMsgOveride (msg, msgData) {
  return {
    topic: this.getTopic(msg),
    messages: [msgData],
    key: msg.key || this.getKey(msg),
    partition: msg.partition || this.partition || 0,
    attributes: msg.attributes || this.attributes || 0
  }
}
function convert2KafkaMsgNoOveride (msg, msgData) {
  return {
    topic: this.getTopic(msg),
    messages: [msgData],
    key: this.getKey(msg),
    partition: this.partition || 0,
    attributes: this.attributes || 0
  }
}
function processMessageNoCompression (msg, msgData) {
  logger.active&&logger.send({ label: 'processMessageNoCompression', msg: msg })
  try {
    this.whenUp(this.sendKafka, this.convert2Kafka(msg, msgData))
  } catch (ex) {
    logger.active&&logger.sendErrorAndStackDump(ex.message, ex)
    this.error('input error:' + ex.message, msg)
  }
}
function processMessageCompression (msg, msgData, _this = this) {
  if (typeof msgData !== 'string') {
    if (!(msgData instanceof Buffer || msgData instanceof Uint8Array)) {
      _this.processMessageNoCompression(msg, msgData)
      return
    }
  }
  this.compressor.compress(msgData,
    (compressed) => _this.processMessageNoCompression(msg, compressed),
    () => {
      if ((_this.compressionError++) === 1) {
        _this.warn('compression failure(s)')
      }
      try {
        _this.processMessageNoCompression(msg, msgData)
      } catch (ex) {
        logger.active&&logger.sendErrorAndStackDump(ex.message, ex)
        _this.error('input error:' + ex.message, msg)
      }
    }
  )
}

module.exports = function (RED) {
  function KafkaProducerNode (n) {
    RED.nodes.createNode(this, n)
    try {
      const node = Object.assign(this, n, {
        payload:"payload",
        payloadType: "msg",
        compressionError: 0,
        deadletters: 0,
        errorDiscarded: 0,
        messageCount: 0,
        convert2Kafka: (this.msgOveride === true ? convert2KafkaMsgOveride.bind(this) : convert2KafkaMsgNoOveride.bind(this)),
        processMessageCompression: processMessageCompression.bind(this),
        processMessageNoCompression: processMessageNoCompression.bind(this),
        getTopic: (this.msgTopicOveride === true ? getTopicNodeDefault.bind(this) : getTopicNode).bind(this),
        retryCount: 0,
        sendDeadletter: (this.topicDeadletter ? sendDeadletterNewOK.bind(this) : sendDeadletterNotOK.bind(this)),
        sendDeadletterError: sendDeadletterError.bind(this),
        sendKafka: sendKafka.bind(this),
        showStatus: showStatus.bind(this),
        successes: 0,
        options: {
          // Configuration for when to consider a message as acknowledged, default 1
          requireAcks: this.requireAcks || 1,
          // time in milliseconds to wait for all acks before considered, default 100ms
          ackTimeoutMs: this.ackTimeoutMs || 100,
          // Partitioner type (default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4), default 0
          partitionerType: this.partitionerType || 0
        }
      })
      node._base=new noderedBase(RED,node);
      this._base.setSource("payload")

      node.brokerNode = RED.nodes.getNode(this.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      const host=node.brokerNode.hostState;
      nodeStatus(node)
      node.client = node.brokerNode.getClient()
      const client = node.client
      client
      .beforeDown(next => {
        node.status({ fill: 'red', shape: 'ring', text: 'client coming down'})
        if(node.isAvailable()) node.setDown()
        next()
      }).onDown(next => {
        logger.active&&logger.info({ label: 'client.onDown', node: node.id, name: node.name })
        nodeStatus(node)
        next()
      }).onUp(next => {
        logger.active&&logger.info({ label: 'client.onUp', node: node.id, name: node.name })
        nodeStatus(node)
        node.setUp()
        next()
      })
      host
      .onUp(next => {
        logger.active&&logger.info({ label: 'host.onUp', node: node.id, name: node.name })
        client.setUp(next=>{
          logger.active&&logger.info({ label: 'client.setUp', node: node.id, name: node.name })
          nodeStatus(node)
          next()
        })
        nodeStatus(node)
        next()
      }).onDown(next => {
        logger.active&&logger.info({ label: 'host.onDown', node: node.id, name: node.name })
        nodeStatus(node)
        next()
      })

      this.state = new State(this)
      this.state
        .onUp(next=> {
          logger.active&&logger.info({ label: 'onUp', node: node.id, name: node.name })
          nodeStatus(node)
          node.maxQState = false
          next()
        }).onDown(next => {
          logger.active&&logger.info({ label: 'onDown', node: node.id, name: node.name })
          nodeStatus(node)
          next()
        }).onError((error,next)=>{
          logger.error({ label: 'onError', node: node.id, name: node.name, error:error })
          node.status({ fill: 'red', shape: 'ring', text: error })
          next()
        }).setUpAction(next => {
          logger.active&&logger.info({ label: 'upAction', node: node.id, name: node.name })
          node.status({ fill: 'yellow', shape: 'ring', text: 'connecting' })
          const kafka = node.brokerNode.getKafkaDriver()
          node.producer = new kafka[(node.connectionType || 'Producer')](client.connection, node.options)
          node.producer.on('error', function (ex) {
            const errMsg = node.brokerNode.getRevisedMessage(ex.message)
            logger.error({ label: 'producer.on.error', node: node.id, name: node.name, error: ex.message, errorEnhanced: errMsg })
            if (ex.message.startsWith('Request timed out')) {
              node.status({ fill: 'red', shape: 'ring', text: 'timed out' })
            } else if (ex.message.startsWith('refreshBrokerMetadata failed')) {
              node.refreshBrokerMetadataTimestamp = new Date()
              return
            }
            if (node.isAvailable()) {
              try {
                logger.error({ label: 'producer.on.error isAvaiable so setDown', node: node.id, name: node.name })
                node.setDown(errMsg)
              } catch (ex) {
                node.error('on error for down: ' + ex.message)
              }
            }
          })
          node.producer.on('close', ()=> {
            logger.warn({ label: 'producer.on.close unavailable', node: node.id, name: node.name })
            node.down()
          })
          if (node.producer && node.producer.hasOwnProperty('ready')) {
            node.available()
          } else {
            node.producer.on('ready', function () {
              logger.active&&logger.info({ label: 'producer.on.ready', node: node.id, name: node.name })
              node.available()
            })
          }
          next()
        }).setDownAction(next =>{
          logger.active&&logger.info({ label: 'downAction', node: node.id, name: node.name })
          node.producer.close((err, data)=>{
            logger.active&&logger.send({ label: 'downAction close ', node: node.id, error: err, data: data })
            nodeStatus(node)
          })
          next()
        }).setMaxQDepth(this.nodeQSize)
        .setOnMaxQUpAction(() => {
          logger.active&&logger.info({ label: 'brokerNode.hostState setOnMaxQUpAction', node: node.id, name: node.name })
          node.errorDiscarded++          
          node.error('max queue')
          if (node.maxQState === true) return
          node.status({ fill: 'red', shape: 'ring', text: 'max queue depth reached ' + node.nodeQSize })
          node.maxQState = true
        })
      if (node.compressionType == null) {
        switch (node.attributes) { // old method conversion
          case 1:
            node.compressionType = 'setGzip'
            node.attributes = 0
            break
          case 2:
            node.compressionType = 'setSnappy'
            node.attributes = 0
            break
          default:
            node.compressionType = 'none'
        }
      }
      if (!node.hasOwnProperty('key-type')) node['key-type'] = 'str'
      node.getKey = node.key ? evalInFunction(node, 'key') : () => undefined
      if (node.compressionType == null || node.compressionType === 'none') {
        node.processMessage = this.processMessageNoCompression
      } else {
        // eslint-disable-next-line new-cap
        node.compressor = new compressionTool()
        node.compressor[node.compressionType]()
        node.processMessage = this.processMessageCompression
      }

      if (node.convertFromJson) {
        node.getMessage = (RED, node, msg) => JSON.stringify(node.getPayload(msg))
      } else {
        node.getMessage = (RED, node, msg) => {
          const data = node.getPayload(msg)
          if (data == null) return null
          const dataType = typeof data
          if (['string', 'number'].includes(dataType) ||
            data instanceof Buffer) return data
          if (dataType === 'object') return JSON.stringify(data)
          if (data.buffer) { return Buffer.from(data.buffer) }
          throw Error('unknown data type: ' + dataType)
        }
      }
      logger.active&&logger.send({ label: 'kafkaProducer', node: node.id, getKey: node.getKey.toString() })
      node.on('input', function (msg) {
        node.messageCount++
        try {
          node.processMessage(msg, node.getMessage(RED, node, msg))
        } catch (ex) {
          logger.active&&logger.sendErrorAndStackDump(ex.message, ex)
          node.error('input error:' + ex.message, msg)
        }
      })
      node.on('close', function (removed, done) {
        node.setDown()
        done()
      })
    } catch (ex) {
      this.status({ fill: 'red', shape: 'ring', text: ex.toString() })
      logger.sendErrorAndStackDump(ex.message, ex)
      this.error(ex.toString())
    }
  }
  RED.nodes.registerType(logger.label, KafkaProducerNode)
  setupHttpAdmin(RED, logger.label, {
    checkState: (RED, node, done) => {
      try {
        node.testConnected()
        done('connected')
      } catch (ex) {
        done(ex.message)
      }
    },
    'Clear Queue': (RED, node, done) => {
      node.state.clearWhenUpQ((msg) => node.error('cleared q', msg))
      done('Queue cleared')
    },
    Close: (RED, node, done) => {
      try {
        node.testConnected()
        logger.warn('httpadmin close issued')
        node.producer.close(function (err, data) {
          logger.active&&logger.send({ label: 'kafkaProducer httpadmin close done', node: node.id, error: err, data: data })
          done(err || 'Closed')
        })
      } catch (ex) {
        if (node.producer && node.producer.hasOwnProperty('close')) {
          done(ex.message)
        } else {
          done('not connected')
        }
        done(ex.message)
      }
    },
    Connect: (RED, node, done) => {
      try {
        node.testDisconnected()
        node.brokerNode.testCanConnect()
        node.setUp()
        done('connect issued')
      } catch (ex) {
        logger.active&&logger.send({ label: 'kafkaProducer httpadmin connect', node: node.id, error: ex.message, stack: ex.stack })
        done(ex.message)
      }
    },
    refreshMetadata: (RED, node, done) => {
      try {
        logger.warn('httpadmin refreshMetadata issued')
        const client = node.brokerNode.client
        if (client.isNotAvailable()) {
          done('broker down')
          return
        }
        if (client.connection.refreshMetadata == null) throw Error('no refreshMetadata')
        client.connection.refreshMetadata([node.topic], (err) => {
          const message = err || 'Refreshed'
          logger.warn(message)
          done(message)
          if (node.getUpQDepth()) node.setUp()
        })
      } catch (ex) {
        logger.sendErrorAndStackDump(ex.message, ex)
        done(ex.message)
      }
    },
    'Reset Status': (RED, node, done) => {
      //      node.nodeState.show()
      done('Status reset')
    },
    'Retry Q': (RED, node, done) => {
      if (node.getUpQDepth() === 0) done('Empty Q')
      node.isNotAvailable()
      node.whenUp(() => {
        done('Processed waiting q')
      })
    },
    // eslint-disable-next-line standard/no-callback-literal
    Status: (RED, node, callback) => callback({
      'messages in': node.messageCount,
      retryCount: node.retryCount,
      successes: node.successes,
      deadletters: node.deadletters,
      errorDiscarded: node.errorDiscarded,
      node: node.getState(),
      client: node.client.getState(),
      host: node.brokerNode.hostState.getState()
    })
  })
}
