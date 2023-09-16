const logger = new (require('node-red-contrib-logger'))('Kafka Consumer')
logger.sendInfo('Copyright 2020 Jaroslav Peter Prib')
// const getDataType = require('./getDataType.js')
// const zlib = require('node:zlib');
const setupHttpAdmin = require('./setupHttpAdmin.js')
const State = require('./state.js')

function setStatus (message, fill = 'green') {
  this.status({ fill: fill, shape: 'ring', text: message })
}

function onChangeMetadata (change) {
  if (logger.active) logger.send({ label: 'onChangeMetadata', node: this.id, change: change, filters: this.filters.length })
  const node = this
  const removeTopics = change.remove
  const addTopics = change.add.filter(cell => node.filters.find(regex => regex.test(cell.topic)))
  if (logger.active) logger.send({ label: 'onChangeMetadata actions', node: this.id, add: addTopics, remove: removeTopics })
  if (!node.consumer) {
    if (addTopics.length + addTopics.length === 0) return
    logger.warn({ label: 'onChangeMetadata', id: node.id, warn: 'consumer down', topics: node.activeTopics, remove: removeTopics, add: addTopics })
    this.activeTopics = this.activeTopics.filter(p => !removeTopics.find(c => p.topic === c.topic && c.partition === p.partition))
    this.activeTopics.push(...addTopics)
    return
  }
  if (addTopics.length > 0) {
    logger.warn({ label: 'onChangeMetadata add topics', id: node.id, topics: node.activeTopics, add: addTopics })
    node.addTopics(addTopics, undefined, err => {
      if (err) {
        node.error('auto add topics for ' + JSON.stringify(addTopics) + ' error:' + err)
      }
    })
  } else if (removeTopics.length > 0) { // else to only allow one at a time
    logger.warn({ label: 'onChangeMetadata remove topics', id: node.id, topics: node.activeTopics, remove: removeTopics })
    node.removeTopics(removeTopics, undefined, err => {
      if (err) {
        node.error('auto remove topics for ' + JSON.stringify(addTopics) + ' error:' + err)
      }
    })
  }
}

module.exports = function (RED) {
  function KafkaConsumerNode (n) {
    RED.nodes.createNode(this, n)
    try {
      this.state = new State(this)
      const node = Object.assign(this, n, {
        autoCommitBoolean: (n.autoCommit || 'true') === 'true',
        setStatus: setStatus.bind(this),
        options: {
          groupId: this.groupId || 'kafka-node-group',
          autoCommit: this.autoCommitBoolean,
          autoCommitIntervalMs: this.autoCommitIntervalMs,
          fetchMaxWaitMs: this.fetchMaxWaitMs,
          fetchMinBytes: this.fetchMinBytes,
          fetchMaxBytes: this.fetchMaxBytes,
          fromOffset: this.fromOffset,
          encoding: this.encoding,
          keyEncoding: this.keyEncoding
        }
      })
      this.state
        .onUp(() => {
          if (node.paused) {
            node.log('state changed to up and in paused state')
            node.paused()
          } else {
            node.log('state changed to up, resume issued')
            node.resume()
          }
        }).onDown(() => node.status({ fill: 'red', shape: 'ring', text: 'down' }))
        .setUpAction(() => {
          node.status({ fill: 'yellow', shape: 'ring', text: 'Connecting' })
          node.messageCount = 0
          if (logger.active) logger.send({ label: 'consumer connecting', node: node.id, name: node.name })
          node.consumer = new node.brokerNode.Kafka.Consumer(node.client.connection, node.activeTopics, node.options)
          node.consumer.on('message', (message) => {
            if (logger.active) logger.send({ label: 'consumer on.message', node: node.id, name: node.name })
            try {
              if (++node.messageCount === 1 || node.timedout) {
                node.timedout = false
                node.status({ fill: 'green', shape: 'ring', text: 'Processing Messages' })
                if (message.value == null) return // seems to send an empty on connect in no messages waiting
              } else if (node.messageCount % 100 === 0) node.setStatus('processed ' + node.messageCount)
              node.brokerNode.sendMsg(node, message)
            } catch (ex) {
              logger.sendErrorAndStackDump(ex.message, ex)
              node.paused()
              this.status({ fill: 'red', shape: 'ring', text: 'Error and paused' })
            }
          })
          node.consumer.on('error', function (ex) {
            const err = ex.message ? ex.message : ex.toString()
            if (logger.active) logger.send({ label: 'consumer on.error', node: node.id, name: node.name, error: err })
            node.setError(err)
            if (err.startsWith('Request timed out')) {
              node.setStatus(err, 'yellow')
              node.timedout = true
              return
            }
            node.setStatus(node.brokerNode.getRevisedMessage(err), 'red')
          })
          node.consumer.on('offsetOutOfRange', (ex) => {
            if (logger.active) logger.send({ label: 'consumer on.offsetOutOfRange', node: node.id, error: ex })
            node.consumer.pause()
            node.setStatus('offsetOutOfRange ' + ex.message + ' (PAUSED)', 'red')
          })
          node.consumer.on('ready', () => {
            if (logger.active) logger.send({ label: 'consumer on.ready', node: node.id, name: node.name })
            node.status({ fill: 'green', shape: 'ring', text: 'Ready' })
          })
          node.available()
        }).setDownAction(() => {
          if (logger.active) logger.send({ label: 'close', node: node.id })
          node.setStatus('closing', 'red')
          node.consumer.close(false, () => {
            if (logger.active) logger.send({ label: 'close close', node: node.id })
            delete node.consumer
            node.down()
          })
        })
      node.brokerNode = RED.nodes.getNode(node.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.brokerNode.onChangeMetadata(onChangeMetadata.bind(node))

      this.client = node.brokerNode.getClient()
      this.client.onUp(() => {
        node.setStatus('client up', 'yellow')
        node.log('client connected, connection')
        node.setUp()
      }).onDown(() => {
        node.setStatus('client down', 'red')
        if (node.isAvailable()) node.forceDown()
      }).beforeDown(() => node.setDown())
      node.setStatus('broker down', 'red')
      node.brokerNode.hostState.onUp(() => {
        node.setStatus('client down', 'red')
        node.client.setUp()
      }).onDown(() => {
        node.setStatus('broker down', 'red')
        node.client.forceDown()
      }).beforeDown(() => node.client.setDown())

      if (node.regex) {
        node.activeTopics = []
        node.filters = node.topics.map(t => new RegExp(t.topic))
        logger.info({ label: 'regex', node: node.id, topics: node.topics })
        //        node.setStatus('Initialising wildcard topics', 'yellow')
      } else {
        if (!node.topics) node.activeTopics = [{ topic: node.topic, partition: 0 }] // legacy can be removed in future
        node.activeTopics = node.topics
        //        node.setStatus('Initialising', 'yellow')
      }
      node.on('close', function (removed, done) {
        if (logger.active) logger.send({ label: 'on.close', node: node.id })
        node.setStatus('closing', 'red')
        node.consumer.close(false, () => {
          if (logger.active) logger.send({ label: 'on.close consumer.close', node: node.id })
          try {
            node.setDown()
          } catch (ex) {
            node.error('on close ' + ex.message)
          }
          node.connected = false
          delete node.consumer
          done()
        })
      })
      node.pause = (done) => {
        if (logger.active) logger.send({ label: 'pause', node: node.id })
        node.paused = true
        node.consumer.pause()
        node.setStatus('Paused', 'red')
        done && done()
      }
      node.resume = (done) => {
        if (logger.active) logger.send({ label: 'resume', node: node.id })
        node.resumed = true
        node.consumer.resume()
        node.setStatus('Ready')
        done && done()
      }
      node.addTopics = (topics, fromOffset, done) => {
        if (logger.active) logger.send({ label: 'consumer.addTopics', node: node.id, topics: topics, fromOffset: fromOffset })
        node.consumer.addTopics(topics,
          (err, added) => {
            if (logger.active) logger.send({ label: 'consumer.addTopics  done', node: node.id, topics: topics, fromOffset: fromOffset, added: added, error: err })
            if (err) node.error('add topics to consumer failed error:' + err)
            else node.activeTopics.push(...topics)
            done && done(err)
          },
          fromOffset
        )
      }
      node.removeTopics = (topics, done) => {
        if (logger.active) logger.send({ label: 'consumer.removeTopics', node: node.id, topics: topics })
        node.consumer.removeTopics(topics, (err, removed) => {
          if (logger.active) logger.send({ label: 'consumer.removeTopics  done', node: node.id, topics: topics, removed: removed, error: err })
          if (err) node.error('remove topics from consumer failed error:' + err)
          else node.activeTopics = node.activeTopics.filter(p => !topics.find(c => p.topic === c.topic && c.partition === p.partition))
          done && done(err)
        })
      }
      node.commit = () => {
        node.consumer.commit((err, data) => {
          if (logger.active) logger.send({ label: 'commit', node: node.id, error: err, data: data })
        })
      }
      node.setOffset = (topic, partition, offset) => node.consumer.setOffset(topic, partition, offset)
      node.pauseTopics = (topics) => node.consumer.pauseTopics(topics)
      node.resumeTopics = (topics) => node.consumer.resumeTopics(topics)
    } catch (ex) {
      this.status({ fill: 'red', shape: 'ring', text: ex.toString() })
      logger.sendErrorAndStackDump(ex.message, ex)
      this.error(ex.toString())
    }
  }
  RED.nodes.registerType(logger.label, KafkaConsumerNode)
  setupHttpAdmin(RED, logger.label, {
    addTopics: (RED, node, done, params, data) => {
      node.testUp()
      if (node.regex) return done(null, 'wildcard topics')
      if (!data) return done(null, 'no topic(s) specified')
      node.addTopics(data, undefined, err => done(null, err))
    },
    removeTopics: (RED, node, done, params, data) => {
      node.testUp()
      if (node.regex) return done(null, 'wildcard topics')
      if (!data) return done(null, 'no topic(s) specified')
      node.removeTopics(data, undefined, err => done(null, err))
    },
    activeTopics: (RED, node, done) => {
      node.testUp()
      done(node.activeTopics || [])
    },
    allTopics: (RED, node, done) => {
      node.testUp()
      const topics = node.brokerNode.getTopicsPartitions()
      done(topics, topics == null ? 'getTopicsPartitions returned null' : null)
    },
    close: (RED, node, done) => {
      node.testUp()
      node.setDown(done)
    },
    connect: (RED, node, done) => {
      node.testDown()
      if (node.client.isNotAvailable()) {
        node.client.setUp(done)
      } else { node.setUp(done) }
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
    resetForce: (RED, node, done) => {
      try {
        node.setDown()
        done('set down')
      } catch (ex) {
        node.log('Resetting staus to do as set down error ' + ex.message)
        node.resetDown()
        done(null, 'set down failed and set down status')
      }
    },
    resetClientForce: (RED, node, done) => {
      try {
        node.client.setDown()
        done('set client down')
      } catch (ex) {
        node.log('Resetting staus to do as client set down error ' + ex.message)
        node.client.resetDown()
      }
    },
    status: (RED, node, done) => done({
      node: node.getState(),
      client: node.client.getState(),
      host: node.brokerNode.hostState.getState()
    })
  })
}
