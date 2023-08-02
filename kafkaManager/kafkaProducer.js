const logger = new (require('node-red-contrib-logger'))('Kafka Producer')
logger.sendInfo('Copyright 2022 Jaroslav Peter Prib')
const setupHttpAdmin = require('./setupHttpAdmin.js')
const evalInFunction = require('./evalInFunction.js')
const getDataType = require('./getDataType.js')
const State = require('./state.js')
const zlib = require('node:zlib')
const compressionTool = require('compressiontool')
let maxDetailedError = 111
function showStatus(statusText, statusfill) {
  this.statusText = statusText
  this.statusfill = statusfill
  this.status({ fill: this.statusfill, shape: 'ring', text: this.statusText })
}
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
function sendKafka(msg, done) {
  /*
  //	node.KeyedMessage = kafka.KeyedMessage
  //	km = new KeyedMessage('key', 'message'),
  //	payloads = [
  //			{ topic: 'topic1', messages: 'hi', partition: 0 },
  //			{ topic: 'topic2', messages: ['hello', 'world', km] }
  //	];

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
  if (logger.active) logger.send({ label: 'sendKafka', node: _this.id, msg: sendMsgToString, doneProvide: done != null })
  _this.producer.send([msg], (err, data) => {
    if(err) {
      logger.error({ label: 'send', node: _this.id,error:err })
      const errmsg = (typeof err === 'string' ? err : err.message)
      if (errmsg.startsWith('InvalidTopic')) {
        setInError(_this, 'Invalid topic ' + msg.topic)
      } else if(errmsg.startsWith('Broker not available')) {
        _this.brokerNode.setDown()
        _this.whenUp(_this.sendKafka,msg)
        _this.retryCount++
        done && done(err)
        return;
      } else if(errmsg.startsWith('Could not find the leader')) {

        _this.brokerNode.client.refreshMetadata([msg.topic],(error)=>{ // bug workaround
          if(error){
            _this.error('producer.send client.refreshMetadata '+error)
            _this.showStatus('leader reset issue', 'red')
            done && done(err)
          } else {
            _this.log('refreshMetadata issued')
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
        });
/*
        _this.log('issuing loadMetadataForTopics as may be issue with caching')
        _this.brokerNode.client.loadMetadataForTopics([msg.topic], (error, metadata) => {
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
      _this.sendDeadletter(msg,done)
    } else {
      _this.successes++
    }
    done && done(err)
  })
}

function producerSendDELETE(node, msgIn, retry = 0) {
  try {
    node.producer.send([msg],
      function (err, data) {
        if (err) {
          let errmsg = (typeof err === 'string' ? err : err.message)
          if (errmsg.startsWith('Could not find the leader') ||
            errmsg.startsWith('Broker not available')) {
            //            node.log(errmsg)
            node.inError = true
            if (retry > 0) {
              node.retryCount++
              setInError(node, 'retry ' + node.retryCount + ' failed, waiting: ' + node.waiting.length)
              node.sendDeadletter(msg)
              return
            }
            const client = node.brokerNode.client
            if (client.refreshMetadata) {
              node.log('issuing refreshMetadata as may be issue with caching')
              client.loadMetadataForTopics([], (error, metadata) => {
                if (logger.active) logger.send({ label: 'producer.send client.refreshMetadata', node: node.id, name: node.name, error: err })
                if (err) {
                  setInError(node, errmsg)
                  msg.error = err
                  node.sendDeadletter(msg)
                  return
                }
                if (node.waiting.length) { node.status({ fill: 'yellow', shape: 'ring', text: node.waiting.length + ' queued messages' }) }
                producerSend(node, msg, ++retry)
                if (node.waiting.length) {
                  node.showStatus('retry send to Kafka failed, ' + node.waiting.length + ' queued messages', 'red')
                } else {
                  node.showStatus('Connected to ' + node.brokerNode.name)
                }
              })
              return
            } else if (errmsg.startsWith('Could not find the leader')) {
              msg.error = err
              node.sendDeadletter(msg)
              return
            } else {
              node.connected = false
              node.queueMsg(msg)
            }
          } else if (errmsg.startsWith('InvalidTopic')) {
            errmsg = 'Invalid topic ' + msg.topic
          } else {
            if (logger.active) {
              logger.error({
                label: 'producer.send err',
                node: node.id,
                node: node.name,
                error: errmsg,
                retry: retry,
                messageAttributes: messageAttributes(msg),
                stack: (typeof err === 'string' ? undefined : err.stack)
              })
            }
          }
          setInError(node, errmsg)
          msg.error = err
          node.sendDeadletter(msg)
        } else {
          node.successes++
          if (node.inError) {
            node.inError = false
            _this.showStatus('Connected')
          }
          if (node.waiting) producerSend(node)
        }
      })
  } catch (ex) {
  }
}
function setInError(node, errmsg) {
  node.status({ fill: 'yellow', shape: 'ring', text: 'send error ' + ex.toString() })
}
function sendDeadletterError(msg, err, _this = this) {
  _this.error('dead letter failed turned off')
  _this.sendDeadletter = sendDeadletterNotOK.bind(_this)
  _this.sendDeadletter(msg)
}
function sendDeadletterNewOK(msg, _this = this) {
  try {
    const message = JSON.stringify(msg)
    zlib.gzip(message, (err, buffer) => {
      if (err) {
        logger.error({ label: 'deadletter gzip fail', error: err, deadletterTopic: _this.deadletterTopic })
        _this.sendDeadletterError.apppy(_this, [msg, err])
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

function sendDeadletterNotOK(msg) {
  this.errorDiscarded++
  this.error('discarded', msg)
}
function sendMsgToString(msg) {
  return { topic: msg.topic, key: msg.key, partition: msg.partition, attributes: msg.attributes }.toString()
}
function getTopicNodeDefault(msg) {
  const msgTopic = this.topicSlash2dot && msg.topic ? msg.topic.replace('/', '.') : msg.topic
  return msgTopic || this.topic || ''
}
function getTopicNode() { return this.topic || '' }
function convert2KafkaMsgOveride(msg,msgData){
  return {
    topic: this.getTopic(msg),
    messages: [msgData],
    key: msg.key || this.getKey(msg),
    partition: msg.partition || this.partition || 0,
    attributes: msg.attributes || this.attributes || 0
  }
}
function convert2KafkaMsgNoOveride(msg,msgData){
  return {
    topic: this.getTopic(msg),
    messages: [msgData],
    key:this.getKey(msg),
    partition: this.partition || 0,
    attributes: this.attributes || 0
  }
}
function processMessageNoCompression(msg, msgData) {
  if (logger.active) logger.send({ label: 'processMessageNoCompression', msg: msg })
  try {
/*
    const sendMsg = {
      topic: this.getTopic(msg),
      messages: [msgData],
      key: msg.key || this.getKey(msg),
      partition: msg.partition || this.partition || 0,
      attributes: msg.attributes || this.attributes || 0
    }
*/
    this.whenUp(this.sendKafka, this.convert2Kafka(msg, msgData))
  } catch (ex) {
    if (logger.active) logger.sendErrorAndStackDump(ex.message, ex)
    this.error('input error:' + ex.message, msg)
  }
}
function processMessageCompression(msg, msgData, _this = this) {
  this.compressor.compress(msgData,
    (compressed) => _this.processMessageNoCompression(msg, compressed),
    (err) => {
      if ((_this.compressionError++) == 1) {
        _this.warn('compression failure(s)')
      }
      try{
        _this.processMessageNoCompression(msg, msgData)
      } catch(ex){
        if (logger.active) logger.sendErrorAndStackDump(ex.message, ex)
        _this.error('input error:' + ex.message, msg)
      }
    }
  )
}

module.exports = function (RED) {
  function KafkaProducerNode(n) {
    RED.nodes.createNode(this, n)
    try {
      const node = Object.assign(this, n, {
        compressionError: 0,
        deadletters: 0,
        errorDiscarded: 0,
        messageCount: 0,
        convert2Kafka:(this.msgOveride == true ? convert2KafkaMsgOveride.bind(this):convert2KafkaMsgNoOveride.bind(this)),
        processMessageCompression: processMessageCompression.bind(this),
        processMessageNoCompression: processMessageNoCompression.bind(this),
        getTopic: (this.msgTopicOveride == true ? getTopicNodeDefault.bind(this) : getTopicNode).bind(this),
        retryCount: 0,
        sendDeadletter: (this.topicDeadletter ? sendDeadletterNewOK.bind(this) : sendDeadletterNotOK.bind(this)),
        sendKafka:sendKafka.bind(this),
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
      this.state = new State(this)
      this.state
        .onUp(() => {
          node.status({ fill: 'green', shape: 'ring', text: 'Up' })
          node.maxQState == false
        }).onDown(() => {
          node.status({ fill: 'red', shape: 'ring', text: 'Down' })
        }).onError((error) =>
          node.status({ fill: 'red', shape: 'ring', text: error })
        ).setUpAction(() => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'Connecting' })
          const kafka = node.brokerNode.getKafkaDriver()
          node.producer = new kafka[(node.connectionType || 'Producer')](node.brokerNode.client, node.options)
          node.producer.on('error', function (ex) {
            const errMsg = 'connect error ' + _this.brokerNode.getRevisedMessage(ex.message)
            logger.error({ label: 'producer.on.error', node: node.id, error: ex.message, errorEnhanced: errMsg })
            node.down(errMsg)
          })
          node.producer.on('close', function () {
            node.down()
          })
          if (node.producer && node.producer.hasOwnProperty('ready')) {
            node.available()
          } else {
            node.producer.on('ready', function () {
              if (logger.active) logger.info({ label: 'producer.on.ready', node: node.id, name: node.name })
              node.available()
            })
          }
        }).setDownAction(() =>
          node.producer.close(function (err, data) {
            if (logger.active) logger.send({ label: 'close ', node: node.id, error: err, data: data })
          })
        ).setMaxQDepth(this.nodeQSize)
        .setOnMaxQUpAction(() => {
          node.errorDiscarded++
          node.error('max queue')
          if (node.maxQState == true) return
          node.status({ fill: 'red', shape: 'ring', text: 'max queue depth reached ' + node.nodeQSize })
          node.maxQState == true;
        })
      node.brokerNode = RED.nodes.getNode(this.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.brokerNode
        .onUp(() => {
          node.status({ fill: 'yellow', shape: 'ring', text: "broker available" })
          node.setUp()
        }).onDown(() => {
          node.status({ fill: 'red', shape: 'ring', text: "broker down" })
          node.setDown();
        })
      if (node.compressionType == null) {
        switch (node.attributes) { // old method conversion
          case 1:
            node.compressionType = 'setGzip'
            node.attributes = 0
            break
          case 2: arguments
            node.compressionType = 'setSnappy'
            node.attributes = 0
            break
          default:
            node.compressionType = 'none'
        }
      }
      if (!node.hasOwnProperty('key-type')) node['key-type'] = 'str'
      node.getKey = node.key ? evalInFunction(node, 'key') : () => undefined
      if (node.compressionType == null || node.compressionType == 'none') {
        node.processMessage = this.processMessageNoCompression
      } else {
        node.compressor = new compressionTool(),
          node.compressor[node.compressionType]
        node.processMessage = this.processMessageCompression
      }
      if (node.convertFromJson) {
        node.getMessage = (RED, node, msg) => JSON.stringify(msg.payload)
      } else {
        node.getMessage = (RED, node, msg) => {
          const data = msg.payload
          if (data == null) return null
          const dataType = typeof data
          if (['string', 'number'].includes(dataType) ||
            data instanceof Buffer) return msg.payload
          if (dataType == 'object') return JSON.stringify(msg.payload)
          if (data.buffer) { return Buffer.from(data.buffer) }
          throw Error('unknow data type: ' + dataType)
        }
      }
      if (logger.active) logger.send({ label: 'kafkaProducer', node: node.id, getKey: node.getKey.toString() })
      node.on('input', function (msg) {
        node.messageCount++;
        try {
          node.processMessage(node.getMessage(RED, node, msg))
        } catch (ex) {
          if (logger.active) logger.sendErrorAndStackDump(ex.message, ex)
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
    Status: (RED, node, callback) => callback({
      "messages in": node.messageCount,
      retryCount: node.retryCount,
      successes: node.successes,
      deadletters: node.deadletters,
      errorDiscarded: node.errorDiscarded,
      node: node.getState(),
      client: node.brokerNode.getState(),
      host: node.brokerNode.hostState.getState()
    }),
    Connect: (RED, node, callback) => {
      try {
        node.testDisconnected()
        node.brokerNode.testCanConnect()
        node.setUp()
        callback('connect issued')
      } catch (ex) {
        if (logger.active) logger.send({ label: 'kafkaProducer httpadmin connect', node: node.id, error: ex.message, stack: ex.stack })
        callback(ex.message)
      }
    },
    Close: (RED, node, callback) => {
      try {
        node.testConnected()
        logger.warn('httpadmin close issued')
        node.producer.close(function (err, data) {
          if (logger.active) logger.send({ label: 'kafkaProducer httpadmin close callback', node: node.id, error: err, data: data })
          callback(err || 'Closed')
        })
      } catch (ex) {
        if (node.producer && node.producer.hasOwnProperty('close')) {
          callback(ex.message)
        } else {
          callback('not connected')
        }
        callback(ex.message)
      }
    },
    'Retry Q': (RED, node, callback) => {
      if (node.getUpQDepth() == 0) callback("Empty Q")
      node.testNotConnected();
      node.whenUp(() => {
        callback('Processed waiting q');
      });
    },
    'Reset Status': (RED, node, callback) => {
      //      node.nodeState.show()
      callback('Status reset')
    },
    'Clear Queue': (RED, node, callback) => {
      node.state.clearWhenUpQ((msg) => node.error('cleared q', msg))
      callback('Queue cleared')
    },
    refreshMetadata: (RED, node, callback) => {
      try {
        logger.warn('httpadmin refreshMetadata issued')
        if(node.brokerNode.isNotAvailable()){
          callback("broker down")
          return
        }  
        const client = node.brokerNode.client
        if (client.refreshMetadata == null) throw Error('no refreshMetadata')
        client.refreshMetadata([node.topic], (err) => {
          const message = err || 'Refreshed'
          logger.warn(message)
          callback(message)
          if(node.getUpQDepth()) node.setUp()
        })
      } catch (ex) {
        logger.sendErrorAndStackDump(ex.message, ex)
        callback(ex.message)
      }
    },
    checkState: (RED, node, callback) => {
      try {
        node.testConnected()
        callback('connected')
      } catch (ex) {
        callback(ex.message)
      }
    }
  })
}
