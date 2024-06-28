function commonConsumerUpAction(node,next,errorCallback,logger){
    node.consumer.on('error', (e) => {
      logger.active&&logger.send({ label: 'on.error', node: node.id, name: node.name, error: e })
      const err = e.message ? e.message : e.toString()
      if (err.startsWith('Request timed out')) {
        node.status({ fill: 'yellow', shape: 'ring', text: e.message })
        node.timedout = true
        errorCallback()
        return
      }
      node.status({ fill: 'red', shape: 'ring', text: node.brokerNode.getRevisedMessage(err) })
    })
    node.consumer.on('message', (message) => {
      logger.active&&logger.send({ label: 'on.message', node: node.id, name: node.name, message: message })
      try {
        if (++node.messageCount === 1 || node.timedout) {
          node.timedout = false
          node.status({ fill: 'green', shape: 'ring', text:  readyState(node,{connection:'Processing Messages'}) })
          if (message.value == null) return // seems to send an empty on connect in no messages waiting
        } else if (node.messageCount % 100 === 0) node.status({ fill: 'green', shape: 'ring', text: 'processed ' + node.messageCount })
        node.brokerNode.sendMsg(node, message)
      } catch (ex) {
        logger.sendErrorAndStackDump(ex.message, ex)
        node.paused()
        node.status({ fill: 'red', shape: 'ring', text: 'Error and paused' })
      }
    })
    node.consumer.on('rebalancing', () => {
      logger.info({ label: 'consumer.on.rebalancing', node: node.id, name: node.name })
    })
    node.consumer.on('offsetOutOfRange', (err) => {
      logger.active&&logger.send({ label: 'consumer.on.offsetOutOfRange', node: node.id, name: node.name, error: err })
      node.consumer.pause()
      node.status({ fill: 'red', shape: 'ring', text: 'offsetOutOfRange ' + err.message + ' (PAUSED)' })
    })
    node.consumer.on('ready', () => { // all brokers discovered
      logger.active&&logger.send({ label: 'consumer on.ready', node: node.id, name: node.name })
      node.status({ fill: 'green', shape: 'ring', text: 'Ready All' })
    })
    node.consumer.on('connect', () => { // broker ready
      logger.active&&logger.send({ label: 'consumer on.connect', node: node.id, name: node.name })
      node.status({ fill: 'green', shape: 'ring', text: 'Ready' })
      node.available()
      next()
    })
  }
  module.exports = commonConsumerUpAction
  