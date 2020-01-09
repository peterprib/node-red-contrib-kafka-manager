const nodeLabel = 'Kafka Admin'
const d = require('./lib/debugOn')
d.debugInit(100, nodeLabel)
const debug = d.debugOn

function msgProcess (node, msg, errObject, data) {
  debug({
    label: 'msgProcess',
    error: errObject,
    data: data
  })
  if (errObject) {
    const err = typeof errObject !== 'string' ? errObject.toString() : errObject.message.toString()
    if (err.startsWith('Broker not available') || err.startsWith('Request timed out')) {
      node.warn('Broker not available, queue message and retry connection')
      node.waiting.push(msg)
      node.brokerNode.connect(node, 'Admin', (err) => {
        node.error('connection failed, clearing waiting queue ' + node.waiting.length)
        errorWaiting(node, err)
      })
      return
    }
    node.error(msg.topic + ' ' + err)
    msg.error = err
    node.send([null, msg])
    return
  }
  switch (msg.topic) {
    case 'createAcls':
    case 'createTopics':
    case 'deleteAcls':
    case 'deleteTopics':
    case 'electPreferredLeaders':
      data.forEach((c, i, a) => {
        const t = msg.payload.find((cp) => cp.topic === c.topic)
        debug({
          label: 'msgProcess multi response',
          topic: c,
          data: t
        })
        if (c.hasOwnProperty('error')) {
          debug({
            label: 'msgProcess multi response',
            data: {
              topic: msg.topic,
              error: c.error,
              payload: [t]
            }
          })
          node.send([null, {
            topic: msg.topic,
            error: c.error,
            payload: [t]
          }])
          return
        }
        debug({
          label: 'msgProcess multi response ok',
          data: {
            topic: msg.topic,
            payload: [c]
          }
        })
        node.send({
          topic: msg.topic,
          payload: [t]
        })
      })
      break
    default:
      msg.payload = data
      node.send(msg)
  }
}
const processInputNoArg = [
  'describeCluster', 'describeDelegationToken', 'describeReplicaLogDirs', 'listConsumerGroups', 'listGroups', 'listTopics'
]
const processInputPayloadArg = [
  'alterConfigs', 'alterReplicaLogDirs', 'createAcls', '', 'createDelegationToken',
  'createPartitions', 'createTopics', 'deleteAcls', 'deleteConsumerGroups', 'deleteRecords',
  'deleteTopics', 'describeAcls', 'describeConsumerGroups',
  'describeGroups', 'describeLogDirs', 'describeTopics', 'electPreferredLeaders',
  'expireDelegationToken', 'incrementalAlterConfigs', 'listConsumerGroupOffsets',
  'renewDelegationToken'
]

function processInput (node, msg) {
  debug({
    label: 'processInput',
    msg
  })
  try {
    if (processInputNoArg.includes(msg.topic)) {
      debug({
        label: 'processInput processInputNoArg',
        msg
      })
      node.connection[msg.topic]((err, data) => msgProcess(node, msg, err, data))
      return
    }
    if (processInputPayloadArg.includes(msg.topic)) {
      debug({
        label: 'processInput processInputPayloadArg',
        msg
      })
      node.connection[msg.topic](msg.payload, (err, data) => msgProcess(node, msg, err, data))
      return
    }
    let resource = {}
    let payload = {}
    switch (msg.topic) {
      case 'describeConfigs':
        // msg.payload={type:'topic',name:'a-topic'}
        resource = {
          resourceType: node.connection.RESOURCE_TYPES[msg.payload.type || 'topic'], // 'broker' or 'topic'
          resourceName: msg.payload.name,
          configNames: [] // specific config names, or empty array to return all,
        }
        payload = {
          resources: [resource],
          includeSynonyms: false // requires kafka 2.0+
        }
        node.connection.describeConfigs(payload, (err, data) => msgProcess(node, msg, err, data))
        break
      default:
        throw Error('invalid message topic')
    }
  } catch (e) {
    debug({
      label: 'processInput catch',
      error: e,
      msg: msg,
      connection: Object.keys(node.connection)
    })
    msg.error = e.toString()
    node.send([null, msg])
  }
}

function adminRequest (node, res, err, data) {
  if (err) {
    node.error(err)
    res.status(500).send(err)
    return
  }
  res.status(200).send(data)
}

function errorWaiting (node, err) {
  while (node.waiting.length) {
    const msg = node.waiting.shift()
    msg.error = err
    node.send([null, msg])
  }
}

module.exports = function (RED) {
  function KafkaAdminNode (n) {
    RED.nodes.createNode(this, n)
    var node = Object.assign(this, n, {
      connected: false,
      waiting: [],
      connecting: false,
      processInput: processInput
    })
    node.brokerNode = RED.nodes.getNode(node.broker)
    node.brokerNode.setState(node)
    node.status({
      fill: 'yellow',
      shape: 'ring',
      text: 'Deferred connection'
    })
    try {
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.on('input', function (msg) {
        if (node.connected) {
          processInput(node, msg)
        } else {
          node.waiting.push(msg)
          if (node.connecting) {
            node.log(node.waiting.length + ' in wait queue')
            return
          }
          node.brokerNode.connect(node, 'Admin', (err) => {
            node.error('connection failed, clearing waiting queue ' + node.waiting.length)
            errorWaiting(node, err)
          })
        }
      })
    } catch (e) {
      node.error(e.toString())
      node.status({
        fill: 'red',
        shape: 'ring',
        text: e.message
      })
      return
    }
    node.on('close', function (removed, done) {
      node.status({
        fill: 'red',
        shape: 'ring',
        text: 'closed'
      })
      node.connection.close(false, () => {
        node.log('closed')
      })
      done()
    })
  }
  RED.nodes.registerType(nodeLabel, KafkaAdminNode)
  RED.httpAdmin.get('/KafkaAdmin/:id/:action/', RED.auth.needsPermission('KafkaAdmin.write'), function (req, res) {
    var node = RED.nodes.getNode(req.params.id)
    if (node && node.type === 'Kafka Admin') {
      if (!node.connected) {
        node.brokerNode.connect(node, 'Admin', (err) => {
          node.error(err)
          res.status(500).send(err)
        })
        return
      }
      try {
        if (processInputNoArg.includes(req.params.action)) {
          node.connection[req.params.action]((err, data) => adminRequest(node, res, err, data))
          return
        }
        throw Error('unknown action: ' + req.params.action)
      } catch (err) {
        debug({
          label: 'httpAdmin',
          error: err,
          request: req.params,
          connection: Object.keys(node.connection)
        })
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
