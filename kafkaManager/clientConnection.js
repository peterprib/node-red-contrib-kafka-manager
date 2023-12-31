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
      .setDownAction((next) => {
        if (logger.active) logger.send({ label: 'setDownAction' })
        if (_this.connection) this.connection.close(() => next())
        next()
      }).setUpAction((next) => {
        if (logger.active) logger.send({ label: 'setUpAction' })
        try {
          _this.connection = _this.brokerNode.getKafkaClient()
          _this.connection.on('error', function (error) {
            if (logger.active) logger.send({ label: 'setUpAction on.error', error: error })
            _this.upFailedAndClearQ(error,()=>{
              if (logger.active) logger.send({ label: 'setUpAction on.error upFailedAndClearQ',state:_this.getState()})
              next()
            })
          })
          _this.connection.on('ready', function () {
            if (logger.active) logger.send({ label: 'setUpAction on.ready' })
            next&&next()
          })
          if (this.connectAction) {
            if (logger.active) logger.send({ label: 'setUpAction on.connect' })
            _this.connection.on('connect', function () {
              this.connectAction.callFunction(...this.brokersChanged.args)
            })
          }
          _this.connection.on('brokersChanged', function () {
            if (logger.active) logger.send({ label: 'setUpAction on.brokersChanged' })
            this.brokersChanged.run()
          })
        } catch (ex) {
          if (logger.active) logger.send({ label: 'setUpAction error', error: ex.message })
          _this.upFailedAndClearQ(ex.message,next)
        }
      })
    this.brokerNode.onDown(this.forceDown.bind(this))
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
