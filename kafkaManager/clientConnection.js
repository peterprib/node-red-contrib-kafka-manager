const ProcesStack = require('./processStack.js')
const State = require('./state.js')
function ClientConnnection (brokerNode, logger = new (require('node-red-contrib-logger'))('ClientConnnection')) {
  this.brokerNode = brokerNode
  this.logger = logger
  try{
    this.state = new State(this)
    this.brokersChanged = (new ProcesStack()).setNext()
    const _this = this
    this.state
      .setDownAction(next => {
        logger.active&&logger.send({ label: 'setDownAction' })
        if (_this.connection) _this.connection.close(() => next())
        next()
      }).setUpAction(next => {
        logger.active&&logger.send({ label: 'setUpAction' })
        try {
          _this.connection = _this.brokerNode.getKafkaClient()
          _this.connection.on('error', function (error) {
            logger.active&&logger.send({ label: 'setUpAction on.error', error: error })
            _this.upFailedAndClearQ(error,()=>{
              logger.active&&logger.send({ label: 'setUpAction on.error upFailedAndClearQ',state:_this.getState()})
              next()
            })
          })
          _this.connection.on('ready', function () {
            logger.active&&logger.send({ label: 'setUpAction on.ready' })
            next()
          })
          if (_this.connectAction) {
            _this.connection.on('connect', function () {
              logger.active&&logger.send({ label: 'setUpAction on.connect' })
              _this.connectAction.callFunction(..._this.brokersChanged.args)
            })
          }
          _this.connection.on('brokersChanged', function () {
            logger.active&&logger.send({ label: 'setUpAction on.brokersChanged' })
            _this.brokersChanged.run()
          })
        } catch (ex) {
          logger.active&&logger.send({ label: 'setUpAction error', error: ex.message })
          _this.upFailedAndClearQ(ex.message,next)
        }
      })
    this.brokerNode.onDown(next=>{
      logger.active&&logger.send({ label: 'clientConnection onDown'})
      _this.forceDown.call(_this,nextForce=>nextForce())
      next&&next()
    })
  } catch(ex) {
    logger.error({ label: 'error', error: ex.message})
    console.error(ex.stack)
  }
  return this
}
ClientConnnection.prototype.onBrokersChanged = function (callFunction, ...args) {
  this.brokersChanged.add(callFunction, ...args)
}
module.exports = ClientConnnection
