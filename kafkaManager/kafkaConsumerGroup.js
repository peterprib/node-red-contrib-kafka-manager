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
        .onUp((next) => {
          if (node.paused) {
            node.log('state changed to up and in paused state')
            node.paused()
          } else {
            node.log('state changed to up, resume issued')
            node.resume()
          }
          node.status({ fill: 'green', shape: 'ring', text: 'ready' })
          next()
        }).onDown((next) => {
          node.status({ fill: 'red', shape: 'ring', text: 'down' })
          next()
        }).setUpAction((next) => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'Connecting' })
          node.messageCount = 0
          const kafka = node.brokerNode.getKafkaDriver()
          node.consumer = new kafka.ConsumerGroup(getOptions(node), node.topics)
          node.consumer.on('error', (e) => {
            if (logger.active) logger.send({ label: 'consumerGroup on.error', node: node.id, name: node.name, error: e })
            const err = e.message ? e.message : e.toString()
            if (err.startsWith('Request timed out')) {
              node.status({ fill: 'yellow', shape: 'ring', text: e.message })
              node.timedout = true
              return
            }
            node.status({ fill: 'red', shape: 'ring', text: node.brokerNode.getRevisedMessage(err) })
            node.forceDown()
          })
          node.consumer.on('message', (message) => {
            if (logger.active) logger.send({ label: 'consumerGroup on.message', node: node.id, name: node.name, message: message })
            try {
              if (++node.messageCount === 1 || node.timedout) {
                node.timedout = false
                node.status({ fill: 'green', shape: 'ring', text: 'Processing Messages' })
                if (message.value == null) return // seems to send an empty on connect in no messages waiting
              } else if (node.messageCount % 100 === 0) node.status({ fill: 'green', shape: 'ring', text: 'processed ' + node.messageCount })
              node.brokerNode.sendMsg(node, message)
            } catch (ex) {
              logger.sendErrorAndStackDump(ex.message, ex)
              node.paused()
              this.status({ fill: 'red', shape: 'ring', text: 'Error and paused' })
            }
          })
          node.available()
          node.consumer.on('rebalancing', () => {
            logger.info({ label: 'consumerGroup on.rebalancing', node: node.id, name: node.name })
          })
          node.consumer.on('offsetOutOfRange', (err) => {
            if (logger.active) logger.send({ label: 'consumer.on.offsetOutOfRange', node: node.id, name: node.name, error: err })
            node.consumer.pause()
            node.status({ fill: 'red', shape: 'ring', text: 'offsetOutOfRange ' + err.message + ' (PAUSED)' })
          })
          next()
        })
        .setDownAction((next) => {
          if (logger.active) logger.send({ label: 'close', node: node.id, name: node.name })
          node.status({ fill: 'red', shape: 'ring', text: 'Closing' })
          node.consumer.close(false, () => {
            node.status({ fill: 'red', shape: 'ring', text: 'Closed' })
            node.down(next)
          })
        })
      node.brokerNode = RED.nodes.getNode(node.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.status({ fill: 'red', shape: 'ring', text: 'broker down' })
      node.brokerNode.client.onUp((next) => {
        node.status({ fill: 'red', shape: 'ring', text: 'broker client down' })
        node.setUp(next)
      }).onDown((next) => {
        node.status({ fill: 'red', shape: 'ring', text: 'broker client down' })
        if (node.isAvailable()) node.forceDown()
        next()
      })
      node.on('close', function (removed, done) {
        if (logger.active) logger.send({ label: 'close', node: node.id, name: node.name })
        node.setDown(done)
      })
      node.pause = (done) => {
        if (logger.active) logger.send({ label: 'pause', node: node.id, name: node.name })
        node.paused = true
        node.consumer.pause()
        node.status({ fill: 'red', shape: 'ring', text: 'Paused' })
        done && done()
      }
      node.resume = (done) => {
        if (logger.active) logger.send({ label: 'resume', node: node.id, name: node.name })
        node.resumed = true
        node.consumer.resume()
        node.status({ fill: 'green', shape: 'ring', text: 'Ready' })
        done && done()
      }
      node.commit = (done) => {
        node.consumer.commit((err, data) => {
          if (logger.active) logger.send({ label: 'commit', node: node.id, name: node.name, error: err, data: data })
          done && done(data, err)
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
      client: node.brokerNode.client.getState(),
      host: node.brokerNode.hostState.getState()
    })
  })
}
