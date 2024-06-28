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
  if(!brokerNode) throw Error("no broker node specified")
  this.brokerNode = brokerNode
  this.logger = logger
  this.state = new State(this)
  const _this = this
  this.state.setUpAction(next=> {
    if(_this.logger.active) _this.logger.send('AdminConnnection upAction')
    try{
      _this.brokerNode.getConnection('Admin',
      (connection) => {
        if(_this.logger.active) _this.logger.send('AdminConnnection connect')
        _this.connection = connection
        next()
      },
      (error) => {
        _this.logger.error('AdminConnnection getConnection error:' + error)
        _this.upFailed(error,next)
      }
    )
  } catch(ex) {
    _this.logger.error('AdminConnnection getConnection ex:' + ex.message)
    console.error(ex.stack)
    _this.upFailed(ex.message,next)
  }
}).setDownAction(next => {
  if (_this.logger.active) _this.logger.send({ label: 'AdminConnnection downaAction', node: _this.brokerNode.id, consumerConnected:_this.consumer!=null })
  if(_this.consumer) {
    _this.connection.close(false, () => {
      if (_this.logger.active) _this.logger.send({ label: 'AdminConnnection close', node: _this.brokerNode.id })
      delete _this.consumer    
    })
  }
  next()
})
  _this.brokerNode.beforeDown(this.setDown.bind(this))
//  brokerNode.state.onDown(this.forceDown.bind(this))
}

AdminConnnection.prototype.request = function (requestType,
  done = requestType.callback,
  onError = requestType.error??(ex=>{throw ex})
) {
  this.logger.active&&this.logger.send({label:"AdminConnnection request ", requestType:requestType??"*** missing ***"})
  try {
    if (this.isNotAvailable()) throw Error('no connection')
    const action = requestType instanceof Object ? requestType.action : requestType
    if (processInputNoArg.includes(action)) {
      this.connection[action]((error, result) => {
        error?onError(error): done(result)
      })
      return
    }
    const data = requestType.data
    const _this=this
    if (processInputresultArg.includes(action)) {
      this.connection[action](data, (error, result) => {
        _this.logger.active&&_this.logger.send({label:'AdminConnnection request', action:action, error:error, result:result})
        if(error && error.startsWith("Broker not available") ){
          _this.logger.error({label:'AdminConnnection request',error:error, result:result})
          _this.brokerNode.forceDown(next=>{
            _this.logger.error({label:'AdminConnnection request broker forced down'})
            next()
          }) 
        } 
        error? onError(error) : done(result)
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
          error?onError(error):done(result)
        })
        break
      default:
        onError(Eror('invalid request '+action+ " request type: " +JSON.stringify(requestType??"adminConnection.request missinq request")))
    }
  } catch (ex) {
    const brokerDown="Broker not available"
    if(ex.message.startsWith(brokerDown)) {
      this.brokerNode.hosthostState.forceDown()
      onError(Error("Broker not available"))
      return
    }
    if (onError) {
      onError(ex)
      return
    }
    throw ex
  }
}
module.exports = AdminConnnection
