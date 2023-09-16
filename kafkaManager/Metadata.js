const compareArrays = require('./compareArrays.js')
const filterArray = require('./filterArray.js')

const compareTopicsNew = (source, target) => target.filter(s => undefined === source.find(t => s.topic === t.topic && s.partition === t.partition))

function Metadata (broker, logger = new (require('node-red-contrib-logger'))('Metadata')) {
  this.broker = broker
  this.logger = logger
  this.refreshStack = []
  this.topicsPartitions = []
  this.topicsPartitionsPrevious = []
  this.data = { 1: { metadata: [] } }
  this.refresh = this.refreshFunction.bind(this)
  this.startRefresh = this.startRefreshFunction.bind(this)
  this.stopRefresh = this.stopRefreshFunction.bind(this)
  this.history = []
  const _this = this
  broker.client.onUp(() => {
    broker.getConnection('Admin',
      (connection) => { this.connection = connection },
      (error) => _this.logger.error('metadata getConnection error:' + error)
    )
  })
}
Metadata.prototype.compareTopicsLists = function (left, right, filter) {
  const r = this.topics(filter, right)
  const l = this.topics(filter, left)
  return compareArrays(r, l)
}
Metadata.prototype.setTopicPartitions = function () {
  const r = []
  const topics = this.data[1].metadata
  for (const topic in topics) {
    const topicDetails = topics[topic]
    for (const partition in topicDetails) { r.push({ topic: topic, partition: partition }) }
  }
  // if(this.logger.active) this.logger.send({label: 'Metadata.setTopicPartitions', node:this.broker.id,topics:r});
  this.topicsPartitionsPrevious = Object.values(this.topicsPartitions)
  this.topicsPartitions = r
}
Metadata.prototype.getTopicsPartitions = function (all) {
  return all ? this.topicsPartitions : this.topicsPartitions.filter(c => !c.topic.startsWith('__'))
}
Metadata.prototype.onChange = function (callBack) {
  this.refreshStack.push(callBack)
}
Metadata.prototype.refreshFunction = function (next) {
  if (this.logger.active) this.logger.send({ label: 'Metadata refresh', node: this.broker.id, connected: this.broker.connected })
  if (this.broker.client.isNotAvailable()) return
  const node = this
  this.broker.adminRequest({
    action: 'listTopics',
    callback: (data) => {
      if (node.logger.active) node.logger.send({ label: 'Metadata refresh callback', node: node.broker.id })
      node.dataPrevious = { ...node.data }
      node.data = data
      node.setTopicPartitions()
      node.changes = {
        add: compareTopicsNew(node.topicsPartitionsPrevious, node.topicsPartitions),
        remove: compareTopicsNew(node.topicsPartitions, node.topicsPartitionsPrevious)
      }
      if (node.changes.add.length || node.changes.add.length) node.history.push(Object.assign({ time: new Date() }, node.changes))
      else return
      node.logger.info({ label: 'Metadata refresh ', node: node.broker.id, changes: node.changes })
      node.refreshStack.forEach(consumer => consumer(node.changes))
      next && next()
    },
    error: (ex) => {
      node.logger.error({ label: 'Metadata refresh ', error: ex.message, stack: ex.stack })
      next && next()
    }
  })
}
Metadata.prototype.startRefreshFunction = function () {
  this.logger.info('metadata start cyclic refresh')
  const minutes = 1
  this.refreshMetadata = this.refresh.bind(this)
  if (minutes) this.metadataTimer = setInterval(this.refreshMetadata, minutes * 60 * 1000)
  this.logger.info('started metadata refresh every ' + minutes + ' minute(s) ')
  this.refresh()
}
Metadata.prototype.stopRefreshFunction = function () {
  this.logger.info('metadata stop cyclic refresh')
  if (!this.metadataTimer) return
  this.logger.info('stopped metadata refresh')
  clearTimeout(this.metadataTimer)
  delete this.metadataTimer
}
Metadata.prototype.topics = function (filter, topics = Object.keys(this.data[1].metadata)) {
  try {
    if (this.logger.active) this.logger.send({ label: 'Metadata topics', filter: filter, topics: topics })
    if (filter == null) return topics
    return filterArray(topics, filter)
  } catch (ex) {
    if (this.logger.active) this.logger.send({ label: 'Metadata topics', data: this.data, error: ex.message, stack: ex.stack })
    return []
  }
}
module.exports = Metadata
