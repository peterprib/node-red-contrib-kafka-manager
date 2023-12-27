const ProcesStack = require('./processStack.js')
const State = require('./state.js')
function ClientConnnection (brokerNode, logger = new (require('node-red-contrib-logger'))('ClientConnnection')) {
  this.brokerNode = brokerNode
  this.logger = logger
  this.state = new State(this)
  this.brokersChanged = (new ProcesStack()).setNext()
  const _this = this
  this.state
    .setDownAction((down) => {
      if (logger.active) logger.send({ label: 'setDownAction' })
      if (_this.connection) this.connection.close(() => down())
      down()
    }).setUpAction((up) => {
      if (logger.active) logger.send({ label: 'setUpAction' })
      try {
        _this.connection = _this.brokerNode.getKafkaClient()
        _this.connection.on('error', function (error) {
          if (logger.active) logger.send({ label: 'setUpAction on.error', error: error })
          _this.upFailedAndClearQ(error)
        })
        _this.connection.on('ready', function () {
          if (logger.active) logger.send({ label: 'setUpAction on.ready' })
          up&&up()
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
        _this.upFailedAndClearQ(ex.message)
      }
    })
  this.brokerNode.onDown(this.forceDown.bind(this))
  return this
}
ClientConnnection.prototype.onBrokersChanged = function (callFunction, ...args) {
  this.brokersChanged.add(callFunction, ...args)
}
module.exports = ClientConnnection
