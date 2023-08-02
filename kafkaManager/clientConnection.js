const State = require('./state.js');
function ClientConnnection(brokerNode) {
    this.brokerNode=brokerNode
    this.state=new State(this)
    const _this=this
    this.state
    .setDownAction(()=>{
        _this.connection.close(()=>_this.down())
    }).setUpAction(()=>{
        _this.connection = _this.brokerNode.getKafkaClient()
        _this.connection.on('ready', function () {
            _this.available()
        })
        if(this.connect)
            _this.connection.on('connect', function () {
                this.connect.callFunction(...this.brokersChanged.args)
            })
        if(this.brokersChanged)
            _this.connection.on('brokersChanged', function () {
                this.brokersChanged.callFunction(...this.brokersChanged.args)
            })
    })
}
ClientConnnection.prototype.onBrokersChanged = function(callFunction,...args) {
    this.brokersChanged={callFunction:callFunction,args:args}
    if(_this.isAvailable())
        _this.connection.on('brokersChanged', function () {
            this.brokersChanged.callFunction(...this.brokersChanged.args)
        })
}
ClientConnnection.prototype.onConnect = function(callFunction,...args) {
    this.connect={callFunction:callFunction,args:args}
    if(_this.isAvailable())
        _this.connection.on('brokersChanged', function () {
            this.connect.callFunction(...this.brokersChanged.args)
        })
}
module.exports = ClientConnnection;