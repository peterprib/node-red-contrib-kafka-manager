const logger = new (require('node-red-contrib-logger'))('Kafka Broker')
logger.sendInfo('Copyright 2023 Jaroslav Peter Prib')
const Metadata = require('./Metadata.js')
const getDataType = require('./getDataType.js')
const State = require('./state.js')
const hostAvailable = require('./hostAvailable.js')
const zlib = require('node:zlib')
const compressionTool = require('compressiontool')
const kafka = require('kafka-node')
const AdminConnection = require('./adminConnection.js')
require('events').EventEmitter.prototype._maxListeners = 30
function showStatus () {
  this.statusText = statusText
  this.statusfill = statusfill
  this.status({ fill: this.statusfill, shape: 'ring', text: this.statusText })
}
function processData (node, message, callFunction) {
  const dataType = getDataType(message.value)
  switch (dataType) {
    case 'Uint8Array':
    case 'Buffer':
      zlib.unzip(
        message.value, // buffer,
        { finishFlush: zlib.constants.Z_FULL_FLUSH },
        (err, buffer) => {
          if (err) {
            if (logger.active) logger.send({ label: 'consumer.on.message buffer decompress', node: node.id, nodeName: node.name, error: err })
          } else {
            message.value = buffer
          }
          sendMessage2Nodered(node, message)
        })
      break
    default:
      sendMessage2Nodered(node, message)
  }
}

function sendMsg (node, message) {
  if (message.value == null) return //	seems to send an empty on connect if no messages waiting
  if (node.compressionType && node.compressionType !== 'none') {
    if (!node.compressor) {
      node.compressor = new compressionTool()
      node.compressor[node.compressionType]
    }
    node.compressor.decompress(message.value,
      (data) => {
        message.value = data
        sendMsgPostDecompose(node, message)
      },
      (err) => {
        if ((node.compressionError++) == 1) {
          node.warn('decompression failure(s)')
        }
        sendMsgPostDecompose(node, message)
      }
    )
  } else { sendMsgPostDecompose(node, message) }
}

function sendMsgPostDecompose (node, message) {
  if (logger.active) {
    logger.send({
      label: 'sendMsg',
      node: node.id,
      message: {
        valueDataType: getDataType(message.value),
        topic: message.topic,
        offset: message.offset,
        partition: message.partition,
        highWaterOffset: message.highWaterOffset,
        key: message.key
      }
    })
  }

  try {
    if (!node.ready) {
      node.ready = true
      node.status({	fill: 'green',	shape: 'ring', text: 'Ready' })
      if (message.value == null) return //	seems to send an empty on connect in no messages waiting
    }
    if (node.timedout) {
      node.timedout = false
      node.status({	fill: 'green', shape: 'ring', text: 'Ready' })
    }
    const dataType = getDataType(message.value)
    switch (dataType) {
      case 'Uint8Array':
      case 'Buffer':
        zlib.unzip(
          message.value, // buffer,
          { finishFlush: zlib.constants.Z_FULL_FLUSH },
          (err, buffer) => {
            if (err) {
              if (logger.active) logger.send({ label: 'consumer.on.message buffer decompress', node: node.id, nodeName: node.name, error: err })
            } else {
              message.value = buffer
            }
            sendMessage2Nodered(node, message)
          })
        break
      default:
        sendMessage2Nodered(node, message)
    }
    if (node.closeOnEmptyQ &&
    message.offset == (message.highWaterOffset - 1)) {
      if (logger.active) logger.send({ label: 'sendmsg', node: node.id, action: 'closing consumer as q empty' })
      node.log('consumer q empty so closing')
      node.consumer.close(true, function (err, message) {
        if (logger.active) logger.send({ label: 'sendmsg', node: node.id, action: 'closed', error: err, message: message })
        if (err) node.error('close error:' + err)
      })
    }
  } catch (ex) {
    logger.sendErrorAndStackDump('sendmsg catch', ex)
  }
}

function sendMessage2Nodered (node, message) {
  if (node.convertToJson) {
    try {
      message.value = JSON.parse(message.value)
    } catch (ex) {
      message.error = 'JSON parse error: ' + ex.message
    }
  }
  const kafka = {
    topic: message.topic,
    offset: message.offset,
    partition: message.partition,
    highWaterOffset: message.highWaterOffset,
    key: message.key,
    commit: (node.autoCommitBoolean ? (callback) => callback()
      : (callback, callbackError) => {
        node.consumer.commit((err, data) => {
          if (err) {
            callbackError(err)
          } else {
            callback()
          }
        })
      }),
    rollback: (node.autoCommitBoolean ? (callback) => callback()
      : (callback, callbackError) => {
        logger.sendWarning('rollback close')
        node.close(callback, callbackError)
      })
  }
  node.send({
    topic: message.topic || node.topic,
    payload: message.value,
    _kafka: kafka
  })
}
function getClient () {
  this.testCanConnect()
  return this.client
}
function testCanConnect () {
  if (this.hostState.isNotAvailable()) throw Error('host not available')
  this.testConnected()
}

const setConsumerProperties = (consumerNode) => {
  if (!consumerNode.checkInterval) consumerNode.checkInterval = 1000
  if (!consumerNode.autoCommitIntervalMs) consumerNode.autoCommitIntervalMs = 5000
  if (!consumerNode.fetchMaxWaitMs) consumerNode.fetchMaxWaitMs = 100
  if (!consumerNode.fetchMinBytes) consumerNode.fetchMinBytes = 1
  if (!consumerNode.fetchMaxBytes) consumerNode.fetchMaxBytes = 1024 * 1024
  if (!consumerNode.fromOffset) consumerNode.fromOffset = 'latest'
  if (!consumerNode.encoding) consumerNode.encoding = 'utf8'
  if (!consumerNode.keyEncoding) consumerNode.keyEncoding = 'utf8'
  if (!consumerNode.connected) consumerNode.connected = false
  if (!consumerNode.paused) consumerNode.paused = false
  if (!consumerNode.timedout) consumerNode.timedout = false
  if (!consumerNode.decomressionsErrors) consumerNode.decomressionsErrors = 0
}

module.exports = function (RED) {
  function KafkaBrokerNode (n) {
    RED.nodes.createNode(this, n)
    try {
      const node = Object.assign(this, { hosts: [] }, n, {
        adminRequest: (request) => {
          if (logger.active) logger.send({ label: 'adminRequest', action: request.action, properties: Object.keys(request) })
          if (!request.action) throw Error('action not provided')
          if (!request.callback) throw Error('callback not provided')
          if (!request.error) throw Error('error function not provided')
          const adminNode = node.adminConnection
          adminNode.whenUp(adminNode.request.bind(adminNode), request)
        },
        getConnection: (type, okCallback, errorCallback) => {
          const KafkaType = kafka[type]
          const connection = new KafkaType(node.client)
          connection.on('error', function (ex) {
            if (logger.active) logger.send({ label: ' getConnection on.error', type: type, node: node.id, error: ex.message })
            errorCallback(node.getRevisedMessage(ex.message))
          })
          connection.on('connect', function () {
            if (logger.active) logger.send({ label: 'connectKafka.on.connect', type: type, node: node.id })
            okCallback(connection)
          })
        },
        getClient: getClient.bind(this),
        hostsCombined: [],
        sendMsg: sendMsg.bind(this),
        testCanConnect: testCanConnect.bind(this)
      })
      this.state = new State(this)
      this.state
        .onDown(() => {
          logger.error({ label: 'down', id: node.id, name: node.name })
        }).onUp(() => {
          logger.info({ label: 'up', id: node.id, name: node.name })
        }).setDownAction(() => {
          node.client.close(() => { node.down() })
        }).setUpAction(() => {
          if (logger.active) logger.send({ label: 'clientConnect', node: node.id, name: node.name })
          node.client = node.getKafkaClient()
          node.client.on('connect', function () {
            logger.info({ label: 'connectKafka.client.on.connect', node: node.id, name: node.name })
          })
          node.client.on('brokersChanged', function () {
            logger.info({ label: 'connectKafka.client.on.brokersChanged', node: node.id, name: node.name })
          })
          node.client.on('ready', function () {
            logger.info({ label: 'client.on.ready', node: node.id, name: node.name })
            node.available()
          })
        })
      if (node.hostsEnvVar) {
        if (node.hostsEnvVar in process.env) {
          try {
            const hosts = JSON.parse(process.env[node.hostsEnvVar])
            if (hosts) {
              logger.send({ label: 'test', hosts: hosts })
              if (hosts instanceof Array) {
                node.hostsCombined = node.hostsCombined.concat(hosts)
              } else {
                throw Error('not array value: ' + process.env[node.hostsEnvVar])
              }
            }
          } catch (ex) {
            const error = 'process.env variable ' + node.hostsEnvVar + ex.toString()
            throw Error(error)
          }
        } else { throw Error('process.env.' + node.hostsEnvVar + ' not found') }
      }
      if (node.hosts.length === 0 && node.host) {
        node.hosts.push({ host: node.host, port: node.port })
      }
      node.hostsCombined = node.hostsCombined.concat(node.hosts)
      if (node.hostsCombined.length == 0) throw Error('No hosts')
      logger.send({ hosts: node.hostsCombined })
      node.kafkaHost = node.hostsCombined.map((r) => r.host + ':' + r.port).join(',')
      node.getKafkaDriver = () => kafka
      node.getKafkaClient = (optionsOrridden) => {
        const options = Object.assign({
          kafkaHost: node.kafkaHost,
          connectTimeout: node.connectTimeout || 10000,
          requestTimeout: node.requestTimeout || 30000,
          autoConnect: (node.autoConnect || 'true') === 'true',
          idleConnection: node.idleConnection || 5,
          reconnectOnIdle: (node.reconnectOnIdle || 'true') === 'true',
          maxAsyncRequests: node.maxAsyncRequests || 10
        }, optionsOrridden)
        if (logger.active) logger.send({ label: 'getKafkaClient', usetls: node.usetls, options: options })
        if (node.usetls) {
          options.sslOptions = { rejectUnauthorized: node.selfSign }
          if (logger.active) logger.send({ label: 'getKafkaClient tls use', selfSign: node.selfSign })
          try {
            if (!(node.selfServe || node.tls)) throw Error('not self serve or no tls configuration selected')
            if (node.tls) {
              node.tlsNode = RED.nodes.getNode(node.tls)
              if (!node.tlsNode) throw Error('tls configuration not found')
              //      	  	      Object.assign(options.sslOptions,node.tlsNode.credentials);
              node.tlsNode.addTLSOptions(options.sslOptions)
              if (logger.active) logger.send({ label: 'getKafkaClient sslOptions', properties: Object.keys(options.sslOptions) })
            }
          } catch (e) {
            node.error('get node tls ' + node.tls + ' failed, error:' + e)
          }
        }
        if (options.useCredentials) {
          if (logger.active) logger.send({ label: 'getKafkaClient node has configured credentials, note sasl mechanism is plain' })
          options.sasl = {
            mechanism: 'plain',
            username: this.credentials.user,
            password: node.credentials.password
          }
        }
        if (logger.active) logger.send({ label: 'getKafkaClient', options: Object.assign({}, options, options.sslOptions ? { sslOptions: '***masked***' } : null) })
        return ((node.connectViaZookeeper || false) == true)
          ? new kafka.Client(options)
          : new kafka.KafkaClient(options)
      }
      node.getRevisedMessage = (err) => {
        if (typeof err === 'string' && err.startsWith('connect ECONNREFUSED')) return 'Connection refused, check if Kafka up'
        return err
      }
      node.setConsumerProperties = setConsumerProperties
      node.close = function (removed, done) {
        try {
          if (done) node.whenDown(done)
          node.setDown()
        } catch (ex) {
          logger.sendErrorAndStackDump(ex.message, ex)
        }
      }
      node.metadata = new Metadata(node, logger)
      node.onUp(node.metadata.startRefresh.bind(node.metadata))
        .onDown(node.metadata.stopRefresh.bind(node.metadata))
      node.onChangeMetadata = node.metadata.onChange.bind(node.metadata)
      node.metadataRefresh = node.metadata.refresh.bind(node.metadata)
      node.getTopicsPartitions = node.metadata.getTopicsPartitions.bind(node.metadata)
      node.hostState = new hostAvailable(node.hosts, node.checkInterval * 1000)
      node.hostState
        .onDown(() => {
          node.log('state change down')
          node.setDown()
        }).onUp(() => {
          node.log('state change up')
          node.setUp()
        })
      node.adminConnection = new AdminConnection(node)
      node.state
        .onUp(() => node.adminConnection.setUp())
        .onDown(() => node.adminConnection.setDown())
    } catch (ex) {
      this.status({ fill: 'red', shape: 'ring', text: ex.toString() })
      logger.sendErrorAndStackDump(ex.message, ex)
      this.error(ex.toString())
    }
  }
  RED.nodes.registerType(logger.label, KafkaBrokerNode, {
    credentials: {
      user: {
        type: 'text'
      },
      password: {
        type: 'password'
      }
    }
  })
}
