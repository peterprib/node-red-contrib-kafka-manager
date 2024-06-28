const logger = new (require('node-red-contrib-logger'))('Kafka Consumer')
logger.sendInfo('Copyright 2020 Jaroslav Peter Prib')
// const getDataType = require('./getDataType.js')
// const zlib = require('node:zlib');
const setupHttpAdmin = require('./setupHttpAdmin.js')
const State = require('./state.js')
const commonConsumerHostState = require('./commonConsumerHostState.js')
const commonConsumerUpAction = require('./commonConsumerUpAction.js')
function setStatus (message, fill = 'green') {
  this.status({ fill: fill, shape: 'ring', text: message })
}

function onChangeMetadata (change) {
  logger.active&&logger.send({ label: 'onChangeMetadata', node:{id:this.id,name:this.name} , change: change, filters: this.filters })
  const node = this
  const removeTopics = change.remove
  const addTopics = change.add.filter(cell => node.filters.find(regex => regex.test(cell.topic)))
  logger.active&&logger.send({ label: 'onChangeMetadata actions', node:{id:this.id,name:this.name}, add: addTopics, remove: removeTopics })
  if (!node.consumer) {
    if (addTopics.length + addTopics.length === 0) return
    logger.warn({ label: 'onChangeMetadata', node:{id:this.id,name:this.name}, warn: 'consumer down', topics: node.activeTopics, remove: removeTopics, add: addTopics })
    this.activeTopics = this.activeTopics.filter(p => !removeTopics.find(c => p.topic === c.topic && c.partition === p.partition))
    this.activeTopics.push(...addTopics)
    return
  }
  if (addTopics.length > 0) {
    logger.warn({ label: 'onChangeMetadata add topics', node:{id:this.id,name:this.name}, topics: node.activeTopics, add: addTopics })
    node.addTopics(addTopics, undefined, err => {
      if (err) {
        node.error('auto add topics for ' + JSON.stringify(addTopics) + ' error:' + err)
      }
    })
  } else if (removeTopics.length > 0) { // else to only allow one at a time
    logger.warn({ label: 'onChangeMetadata remove topics', node:{id:this.id,name:this.name}, topics: node.activeTopics, remove: removeTopics })
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
        .setUpAction((next,error)=> {
          logger.active&&logger.send({ label: 'consumer connecting', node: node.id, name: node.name })
          node.status({ fill: 'yellow', shape: 'ring', text: 'Connecting' })
          node.messageCount = 0
          node.consumer = new node.brokerNode.Kafka.Consumer(node.brokerNode.client.connection, node.activeTopics, node.options)
          commonConsumerUpAction(node,next,error,logger)
        }).setDownAction(next => {
          logger.active&&logger.send({ label: 'downAction close', node: node.id, name: node.name })
          node.setStatus('closing', 'red')
          node.consumer.close(false, () => {
            logger.active&&logger.send({ label: 'close close', node: node.id, name: node.name })
            delete node.consumer
            node.down()
            node.setStatus('closed', 'red')
            next()
          })
        })
      node.brokerNode = RED.nodes.getNode(node.broker)
      if (!node.brokerNode) throw Error('Broker not found ' + node.broker)
      node.brokerNode.onChangeMetadata(onChangeMetadata.bind(node))
      commonConsumerHostState(node,logger)
      if (node.regex) {
        node.activeTopics = []
        node.filters = node.topics.map(t => new RegExp(t.topic))
        logger.info({ label: 'regex', node: node.id, topics: node.topics })
        //        node.setStatus('Initialising wildcard topics', 'yellow')
      } else {
        node.filters=[]
        if (!node.topics) node.activeTopics = [{ topic: node.topic, partition: 0 }] // legacy can be removed in future
        node.activeTopics = node.topics
        //        node.setStatus('Initialising', 'yellow')
      }
      node.addTopics = (topics, fromOffset, done) => {
        logger.active&&logger.send({ label: 'consumer.addTopics', node: node.id, topics: topics, fromOffset: fromOffset })
        node.consumer.addTopics(topics,
          (err, added) => {
            logger.active&&logger.send({ label: 'consumer.addTopics  done', node: node.id, topics: topics, fromOffset: fromOffset, added: added, error: err })
            if (err) node.error('add topics to consumer failed error:' + err)
            else node.activeTopics.push(...topics)
            done && done(err)
          },
          fromOffset
        )
      }
      node.removeTopics = (topics, done) => {
        logger.active&&logger.send({ label: 'consumer.removeTopics', node: node.id, name: node.name, topics: topics })
        node.consumer.removeTopics(topics, (err, removed) => {
          logger.active&&logger.send({ label: 'consumer.removeTopics  done', node: node.id, name: node.name, topics: topics, removed: removed, error: err })
          if (err) node.error('remove topics from consumer failed error:' + err)
          else node.activeTopics = node.activeTopics.filter(p => !topics.find(c => p.topic === c.topic && c.partition === p.partition))
          done && done(err)
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
        node.client.setUp()
      } else {
        node.setUp()
      }
      done("Connect issued")
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
