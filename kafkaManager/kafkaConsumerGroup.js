const label = 'Kafka Consumer Group'
const ts = (new Date().toString()).split(' ')
console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [info] ' + label + ' Copyright 2019 Jaroslav Peter Prib')

const debugOff = () => false
function debugOn (m) {
  const ts = (new Date().toString()).split(' ')
  if (!debugCnt--) {
    console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [debug] ' + label + ' debugging turn off')
    debug = debugOff
  }
  if (debugCnt < 0) {
    debugCnt = 100
    console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [debug] ' + label + ' debugging next ' + debugCnt + ' debug points')
  }
  console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [debug] ' + label + ' ' + (m instanceof Object ? JSON.stringify(m) : m))
}
let debug = debugOn; let debugCnt = 100

let kafka

function sendMsg (node, message) {
  debug({ label: 'sendMsg', node: node.id, message: message })
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
  debug({ label: 'connect', node: node.id })

  node.options = {
    kafkaHost: node.brokerNode.kafkaHost, // connect directly to kafka broker (instantiates a KafkaClient)
    // batch: undefined, // put client batch settings if you need them
    // ssl: true, // optional (defaults to false) or tls options hash

    groupId: node.groupId,
    sessionTimeout: node.sessionTimeout, // default: 5000,
    protocol: node.protocol, // default: ['roundrobin'],
    encoding: node.encoding, // default: utf8
    fromOffset: node.fromOffset, // default: 'latest'
    commitOffsetsOnFirstJoin: node.commitOffsetsOnFirstJoin === 'true',
    outOfRangeOffset: node.outOfRangeOffset, // default: 'earliest'
    onRebalance: (isAlreadyMember, callback) => { callback() } // or null
  }
  if (node.brokerNode.TLSOptions) {
    node.options.sslOptions = node.brokerNode.TLSOptions
  }
  node.consumer = new kafka.ConsumerGroup(node.options, node.topics)
  node.consumer.on('message', (message) => {
    debug({ label: 'consumer.on.message', node: node.id, message: message })
    if (!node.ready) {
      node.ready = true
      node.status({ fill: 'green', shape: 'ring', text: 'Ready with ' + node.brokerNode.name })
      if (message.value == null) return //  seems to send an empty on connect in no messages waiting
    }
    if (node.timedout) {
      node.timedout = false
      node.status({ fill: 'green', shape: 'ring', text: 'Ready with ' + node.brokerNode.name })
    }
    if (Array.isArray(message)) {
      message.forEach((r) => sendMsg(node, r))
    } else {
      sendMsg(node, message)
    }
  })

  node.consumer.on('error', function (e) {
    debug({ label: 'consumer.on.error', node: node.id, error: e })
    if (e.message.startsWith('Request timed out')) {
      node.status({ fill: 'yellow', shape: 'ring', text: e.message })
      node.log('on error ' + e.message)
      node.timedout = true
      return
    }
    node.error('on error ' + e.message)

    const err = node.brokerNode.getRevisedMessage(e.message)
    node.status({ fill: 'red', shape: 'ring', text: err })
  })
  node.consumer.on('offsetOutOfRange', function (e) {
    debug({ label: 'consumer.on.offsetOutOfRange', node: node.id, error: e })
    node.error('on offsetOutOfRange ' + e)
    node.status({ fill: 'red', shape: 'ring', text: e.message + ' (PAUSED)' })
    node.consumer.pause()
  })
}
module.exports = function (RED) {
  function KafkaConsumerGroupNode (n) {
    RED.nodes.createNode(this, n)
    var node = Object.assign(this, n, { connected: false, paused: false, timedout: false })
    node.brokerNode = RED.nodes.getNode(node.broker)
    node.status({ fill: 'yellow', shape: 'ring', text: 'Initialising' })
    try {
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      if (!kafka) {
        kafka = node.brokerNode.getKafkaDriver()
      }
      node.brokerNode.onStateUp.push({
        node: node,
        callback: function () {
          debug({ label: 'brokerNode.stateUp', node: node.id })
          connect(node)
        }
      }) // needed due to bug in kafka driver
      node.brokerNode.stateUp.push({
        node: node,
        callback: function () {
          debug({ label: 'brokerNode.stateUp', node: node.id })
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
        debug({ label: 'close', node: node.id })
        node.status({ fill: 'red', shape: 'ring', text: 'closed' })
        node.consumer.close(false, () => {
          node.log('closed')
        })
        done()
      })
      node.pause = () => {
        debug({ label: 'pause', node: node.id })
        node.paused = true
        node.consumer.pause()
        node.status({ fill: 'red', shape: 'ring', text: 'paused' })
      }
      node.resume = () => {
        debug({ label: 'resume', node: node.id })
        node.resumed = true
        node.consumer.resume()
        node.status({ fill: 'green', shape: 'ring', text: 'Ready with ' + node.brokerNode.name })
      }
      node.commit = () => {
        node.consumer.commit((err, data) => {
          debug({ label: 'commit', node: node.id, error: err, data: data })
        })
      }
    } catch (e) {
      node.error(e.toString())
      node.status({ fill: 'red', shape: 'ring', text: e.toString() })
    }
  }
  RED.nodes.registerType(label, KafkaConsumerGroupNode)
  RED.httpAdmin.get('/KafkaConsumerGroup/:id/:action/', RED.auth.needsPermission('KafkaConsumerGroup.write'), function (req, res) {
    var node = RED.nodes.getNode(req.params.id)
    if (node && node.type === label) {
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
