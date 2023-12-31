const State = require('./state.js')
const processInputNoArg = ['listConsumerGroups', 'listGroups', 'listTopics']
const processInputresultArg = [
  'alterConfigs', 'alterReplicaLogDirs', 'createAcls', '', 'createDelegationToken',
  'createPartitions', 'createTopics', 'deleteAcls', 'deleteConsumerGroups', 'deleteRecords',
  'deleteTopics', 'describeAcls', 'describeConsumerGroups',
  'describeGroups', 'describeLogDirs', 'describeTopics', 'electPreferredLeaders',
  'expireDelegationToken', 'incrementalAlterConfigs', 'listConsumerGroupOffsets',
  'renewDelegationToken'
]

function AdminConnnection (brokerNode, logger = new (require('node-red-contrib-logger'))('AdminConnnection')) {
  this.brokerNode = brokerNode
  this.logger = logger
  this.state = new State(this)
  const _this = this
  this.state.setUpAction((next) => {
    brokerNode.getConnection('Admin',
      (connection) => {
        _this.connection = connection
        next()
      },
      (error) => {
        _this.logger.error('AdminConnnection getConnection error:' + error)
        brokerNode.state.upFailed(error,next)
      }
    )
  }).setDownAction((next) => {
    _this.connection.close(false, () => {
      if (_this.logger.active) _this.logger.send({ label: 'AdminConnnection close', node: _this.brokerNode.id })
      delete _this.consumer
      next()
    })
  })
  this.brokerNode.onDown(this.forceDown.bind(this))
}

AdminConnnection.prototype.request = function (requestType, done = requestType.callback, onError = requestType.error) {
  try {
    if (this.isNotAvailable()) throw Error('no connection')
    const action = requestType instanceof Object ? requestType.action : requestType
    if (processInputNoArg.includes(action)) {
      this.connection[action]((error, result) => {
        if (error) onError(error)
        done(result)
      })
      return
    }
    const data = requestType.data
    if (processInputresultArg.includes(action)) {
      this.connection[action](data, (_error, result) => {
        if (_error) onError(_error)
        else done(result)
      })
      return
    }
    let resource = {}
    let result = {}
    switch (action) {
      case 'describeConfigs':
        resource = {
          resourceType: this.connection.RESOURCE_TYPES[data.type || 'topic'], // 'broker' or 'topic'
          resourceName: data.name,
          configNames: [] // specific config names, or empty array to return all,
        }
        result = {
          resources: [resource],
          includeSynonyms: false // requires kafka 2.0+
        }
        this.connection.describeConfigs(result, (error, result) => {
          if (error) onError(error)
          else done(result)
        })
        break
      default:
        throw Error('invalid message topic')
    }
  } catch (ex) {
    if (onError) {
      onError(ex)
      return
    }
    throw ex
  }
}
module.exports = AdminConnnection
