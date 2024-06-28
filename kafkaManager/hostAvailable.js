const net = require('net')
const State = require('./state.js')

// host=(host:<ip/uri>,port:<number>)
function hostAvailable (host, checkInterval = 60000, debug) {
  debug&&this.setDebug(debug)
  this.testsOutstanding = 0
  this.timeOuts = 0
  this.hosts = host instanceof Array ? host : [host]
  this.error = console.error
  this.debug && this.debug('hostAvailable hosts: '+this.hosts+" checkInterval "+checkInterval)
    this.state = new State(this)
  this.state.setCheck(this.test.bind(this), checkInterval)
}
hostAvailable.prototype.test = function () {
  this.debug && this.debug('hostAvailable test')
  const _this = this
  this.timeOuts = 0
  this.testsOutstanding = this.hosts.length
  this.hosts.forEach(host => _this.checkHostState(host))
  return this
}
hostAvailable.prototype.hostUnAvailable = function (host) {
  this.debug && this.debug('hostAvailable hostUnAvailable '+host)
  if (--this.testsOutstanding > 0) return this
  if (this.isNotAvailable()) return this
  if (this.hosts.findIndex(host => host.available) < 0) {
    this.debug && this.debug('hostAvailable setDown ')
    this.setDown()
  }
  return this
}
hostAvailable.prototype.setDebug = function (debug=this.debugWas||console.log) {
  this.debug=debug
}
hostAvailable.prototype.setDebugOff = function () {
  this.debugWas=this.debug
  return this
}
hostAvailable.prototype.checkHostState = function (host) {
  this.debug && this.debug('hostAvailable  '+(host.address||host.host)+":"+host.port)
  const _this = this
  if (!host.socket) {
    this.debug && this.debug('hostAvailable checkHostState socket create')
    host.socket = new net.Socket()
    host.socket.on('connect', function () {
      _this.debug && _this.debug('hostAvailable on connect, currently available: '+_this.isAvailable())
      host.socket.destroy()
      if (host.available !== true) {
        host.available = true
        if (_this.isNotAvailable()) {
          try {
            _this.debug && _this.debug('hostAvailable socket on connect setup')
            _this.setUp()
          } catch (ex) { 
            _this.debug && _this.debug('hostAvailable socket on connect setup error'+ex.message)
          }
        }
      }
    }).on('end', function () {
      _this.debug && _this.debug('hostAvailable on end')
    }).on('error', function (_err) {
      _this.debug && _this.debug("hostAvailable on error "+_err)
      _this.hostUnAvailable(host)
    }).on('timeout', function () {
      host.socket.destroy()
      _this.debug && _this.debug('hostAvailable timeout')
      _this.timeOuts++
      _this.hostUnAvailable(host)
    }).setTimeout(100)
  }
  host.socket.connect(host.port, host.address || host.host)
}
module.exports = hostAvailable
