const nodeName="Kafka Broker";
const Logger = require("logger");
const logger = new Logger(nodeName);
logger.sendInfo("Copyright 2020 Jaroslav Peter Prib");

let kafka;

function hostAvailable (host, port, node, availableCB, downCB, timeoutCB) {
  if(logger.active) logger.send({
    label: 'hostAvailable',
    host: host,
    port: port,
    node: node.id
  });
  const net = require('net')
  const socket = new net.Socket()
  socket.on('connect', function () {
	if(logger.active) logger.send({
      label: 'hostAvailable connect',
      host: host,
      port: port,
      node: node.id
    })
    try {
      socket.destroy()
      availableCB.apply(node, [node])
    } catch (e) {
      node.error('hostAvailable on connect error ' + e.message)
    }
  })
    .on('end', function () {
      if(logger.active) logger.send({
        label: 'hostAvailable end',
        host: host,
        port: port,
        node: node.id
      });
    })
    .on('error', function (e) {
      if(logger.active) logger.send({
        label: 'hostAvailable error',
        host: host,
        port: port,
        node: node.id,
        error: e.toString()
      });
      try {
        socket.destroy()
        downCB.apply(node, [e])
      } catch (e) {
        node.error('hostAvailable on error ' + e.message)
      }
    }).on('timeout', function () {
      if(logger.active) logger.send({
        label: 'hostAvailable timeout',
        host: host,
        port: port,
        node: node.id
      });
      try {
        (downCB || timeoutCB).apply(node, ['time out'])
      } catch (e) {
        node.error('hostAvailable on error ' + e.message)
      }
    })
  socket.setTimeout(2000)
  socket.connect(port, host)
};

function stateChange (list, state) {
  list.forEach((r) => {
    r.node.log('state change ' + state)
    r.callback.apply(r.node, [r.args])
  })
};

function testHosts (node) {
  if(logger.active) logger.send({
    label: 'testHosts',
    node: node.id,
    available: node.available
  });
  testHost(node, 0)
}

function testHost (node, i) {
  if (i >= node.hosts.length) {
    if (!node.available) return
    node.available = false
    node.error('state change down')
    stateChange(node.stateDown, 'Down')
    return
  }
  const host = node.hosts[i]
  if(logger.active) logger.send({
    label: 'testHost',
    host: host.host,
    port: host.port,
    node: node.id
  });
  hostAvailable(host.host, host.port, node,
    () => {
      if (node.available) return
      node.available = true
      node.log('state change up, processing ' + node.stateUp.length + ' node scripts')
      stateChange(node.stateUp, 'Up')
      while (node.onStateUp.length) {
        const r = node.onStateUp.shift()
        r.callback.apply(r.node, [r.args])
      }
    },
    () => {
      testHost(node, ++i)
    }
  )
}

function runtimeStop () {
  if (this.runtimeTimer) {
    clearTimeout(this.runtimeTimer)
    this.runtimeTimer = null
  }
  this.status({
    fill: 'red',
    shape: 'ring',
    text: 'Stopped'
  })
  this.log('Monitor Stopped')
}

function setState (node) {
  node.brokerNode.stateUp.push({
    node: node,
    callback: function () {
      this.status({
        fill: 'green',
        shape: 'ring',
        text: 'Available'
      })
    }
  })
  node.brokerNode.stateDown.push({
    node: node,
    callback: function () {
      this.status({
        fill: 'Red',
        shape: 'ring',
        text: 'Down'
      })
    }
  })
}

function connect (node, type, errCB) {
  if(logger.active) logger.send({
    label: 'connect',
    type: type,
    node: node.id,
    available: node.available
  });
  if (node.available) {
    if (errCB) errCB.apply(node, ['connect attempt and not available'])
    return
  }
  if (!node.brokerNode.hosts.length) {
    node.connecting = false
    node.log('host and/or port not defined')
    node.status({
      fill: 'red',
      shape: 'ring',
      text: 'host not defined correctly'
    })
    if (errCB) errCB.apply(node, ['host/port missing'])
    return
  }
  node.connecting = true
  if (node.client) {
    node.status({
      fill: 'yellow',
      shape: 'ring',
      text: 'Reconnecting'
    })
    node.log('Trying to reconnect')
    node.client.connect()
    return
  }
  node.log('Initiating connection')
  node.status({
    fill: 'yellow',
    shape: 'ring',
    text: 'Connecting'
  })
  connectKafka(node, type)
};

function connectKafka (node, type) {
  if(logger.active) logger.send({
    label: 'connectKafka',
    type: type,
    node: node.id,
    host: node.host,
    port: node.port
  });
  node.client = node.brokerNode.getKafkaClient()
  node.connection = new kafka[type](node.client)
  node.connecting = true
  node.connection.on('error', function (e) {
	if(logger.active) logger.send({
      label: 'connectKafka on error',
      node: node.id,
      error: e
    });
    node.connected = false
    node.connecting = false
    const err = node.brokerNode.getRevisedMessage(e.message)
    node.error('on error ' + e.message)
    node.status({
      fill: 'red',
      shape: 'ring',
      text: err
    })
    while (node.waiting.length) {
      const msg = node.waiting.shift()
      msg.error = e.message
      node.send([null, msg])
    }
  })
  node.connection.on('connect', function () {
	if(logger.active) logger.send({
      label: 'connectKafka on connect',
      node: node.id
    });
    node.connected = true
    node.connecting = false
    if(logger.active) logger.send('connected');
    node.status({
      fill: 'green',
      shape: 'ring',
      text: 'Ready'
    })
    node.log('connected and processing ' + node.waiting.length + ' messages')
    if (node.waiting) {
      while (node.waiting.length) {
        node.processInput.apply(node, [node, node.waiting.shift()])
      }
    }
  })
}

module.exports = function (RED) {
  function KafkaBrokerNode (n) {
    RED.nodes.createNode(this, n)
    const node = Object.assign(this, {
      hosts: []
    }, n, {
      available: false,
      connect: connect,
      setState: setState,
      stateUp: [],
      stateDown: [],
      onStateUp: []
    })
    if (node.hosts.length === 0 && node.host) {
      node.hosts.push({
        host: node.host,
        port: node.port
      })
    }
    node.kafkaHost = node.hosts.map((r) => r.host + ':' + r.port).join(',')

    if (node.usetls && node.tls) {
      const tlsNode = RED.nodes.getNode(node.tls)
      if (tlsNode) tlsNode.addTLSOptions(node.TLSOptions)
    }

    node.getKafkaDriver = () => {
      if (!kafka) {
        try {
          kafka = require('kafka-node')
        } catch (e) {
          node.error(e.toString())
          throw e
        }
      }
      return kafka
    }

    node.getKafkaClient = (o) => {
      const options = Object.assign({
        kafkaHost: node.kafkaHost,
        connectTimeout: node.connectTimeout || 10000,
        requestTimeout: node.requestTimeout || 30000,
        autoConnect: (node.autoConnect || 'true') === 'true',
        idleConnection: node.idleConnection || 5,
        reconnectOnIdle: (node.reconnectOnIdle || 'true') === 'true',
        maxAsyncRequests: node.maxAsyncRequests || 10
      }, o)
      if (node.TLSOptions) {
        options.sslOptions = node.TLSOptions
      }
      if (node.credentials.has_password) {
        options.sasl = {
          mechanism: 'plain',
          username: this.credentials.user,
          password: node.credentials.password
        }
      }
      node.getKafkaDriver()
      if(logger.active) logger.send({
        label: 'getKafkaClient',
        options: options
      });
      return new kafka.KafkaClient(options)
    }
    node.getRevisedMessage = (err) => {
      if (typeof err === 'string' && err.startsWith('connect ECONNREFUSED')) return 'Connection refused, check if Kafka up'
      return err
    }
    testHosts(node)
    if (node.checkInterval && node.checkInterval > 0) {
      this.runtimeTimer = setInterval(function () {
        try {
          testHosts.apply(node, [node])
        } catch (e) {
          node.send('runtimeTimer ' + e.message)
        }
      },
      node.checkInterval * 1000
      )
      this.close = function () {
        runtimeStop.apply(node)
      }
    }
  }
  KafkaBrokerNode.prototype.close = function () {
    runtimeStop.apply(this)
  }
  RED.nodes.registerType('Kafka Broker', KafkaBrokerNode, {
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
