const nodeName='Kafka Offset';
const Logger = require("logger");
const logger = new Logger(nodeName);
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");

function msgProcess (node, msg, err, data) {
  if(logger.active) logger.send({
    label: 'msgProcess',
    error: err,
    data: data
  })
  if (err) {
    if (err.startWith('Broker not available')) {
      node.warn('Broker not available, queue message and retry connection')
      node.waiting.push(msg)
      node.brokerNode.connect(node, 'Offset', (err) => {
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
  msg.payload = data
  node.send(msg)
}

function processInput (node, msg) {
  try {
    switch (msg.action || msg.topic) {
      case 'fetch': // Fetch the available offset of a specific topic-partition
        node.connection.fetch(msg.payload,
          (err, data) => msgProcess(node, msg, err, data)
        )
        return
        /*
payloads: Array,array of OffsetRequest, OffsetRequest is a JSON object like:
{
topic: 'topicName',
partition: 0, //default 0
time: Date.now(), // default Date.now(),  Used to ask for all messages before a certain time (ms),
// Specify -1 to receive the latest offsets and -2 to receive the earliest available offset.
maxNum: 1 //default 1
}
*/
      case 'fetchCommits': // Fetch the last committed offset in a topic of a specific consumer group
        node.connection.fetchCommits(msg.groupid || msg.payload.groupid || msg.topic, // groupid: consumer group
          msg.payload.groupid ? msg.payload.topics : msg.payload, // array of OffsetFetchRequest  e.g.  [{topic:'t',partition:0}]
          (err, data) => msgProcess(node, msg, err, data)
        )
        return
      case 'fetchLatestOffsets':
        node.connection.fetch(msg.payload || msg.topic, // topics e.g. [ 'topic1','topic2']
          (err, data) => msgProcess(node, msg, err, data)
        )
        return
      case 'fetchEarliestOffsets':
        node.connection.fetch(msg.payload || msg.topic, // topics e.g. [ 'topic1','topic2']
          (err, data) => msgProcess(node, msg, err, data)
        )
        return
      default:
        throw Error("invalid msg action or topic, e.g msg.action='fetch'")
    }
  } catch (e) {
    if(logger.active) logger.send({
      label: 'input catch',
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
  function KafkaOffsetNode (n) {
    RED.nodes.createNode(this, n)
    var node = Object.assign(this, n, {
      connected: false,
      waiting: [],
      connecting: false,
      processInput: processInput
    })
    node.brokerNode = RED.nodes.getNode(node.broker)
    node.status({
      fill: 'yellow',
      shape: 'ring',
      text: 'Deferred connection'
    })
    try {
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.brokerNode.setState(node)
      node.on('input', function (msg) {
        if (node.connected) {
          processInput(node, msg)
        } else {
          node.waiting.push(msg)
          if (node.connecting) {
            node.log(node.waiting.length + ' in wait queue')
            return
          }
          node.brokerNode.connect(node, 'Offset', (err) => {
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
  RED.nodes.registerType(nodeName, KafkaOffsetNode)
  RED.httpAdmin.get('/KafkaOffset/:id/:action/', RED.auth.needsPermission('KafkaOffset.write'), function (req, res) {
    var node = RED.nodes.getNode(req.params.id)
    if (node && node.type === nodeName) {
      if (!node.connected) {
        node.brokerNode.connect(node, 'Admin', (err) => {
          node.error(err)
          res.status(500).send(err)
        })
        return
      }
      try {
        switch (req.params.action) {
          case 'connect':
            if(logger.active) logger.send({
              label: 'httpadmin connect',
              connection: Object.keys(node.connection)
            })
            node.connection.connect((err, data) => adminRequest(node, res, err, data))
            return
          default:
            throw Error('unknown action: ' + req.params.action)
        }
      } catch (err) {
        if(logger.active) logger.send({
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
