const net = require('net')
const State = require('./state.js')

// host=(host:<ip/uri>,port:<number>)
function hostAvailable (host, checkInterval = 60000, debug) {
  this.debug=debug
  this.testsOutstanding = 0
  this.timeOuts = 0
  this.hosts = host instanceof Array ? host : [host]
  this.error = console.error
  this.state = new State(this)
  this.state.setCheck(this.test.bind(this), checkInterval)
}
hostAvailable.prototype.test = function () {
  const _this = this
  this.timeOuts = 0
  this.testsOutstanding = this.hosts.length
  this.hosts.forEach(host => _this.testHost(host))
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
hostAvailable.prototype.testHost = function (host) {
  this.debug && this.debug('hostAvailable testhost '+(host.address||host.host)+":"+host.port)
  const _this = this
  if (!host.socket) {
    this.debug && this.debug('hostAvailable testhot create socket')
    host.socket = new net.Socket()
    host.socket.on('connect', function () {
      this.debug && this.debug('hostAvailable on connect')
      host.socket.destroy()
      if (host.available !== true) {
        host.available = true
        if (_this.isNotAvailable()) {
          try {
            this.debug && this.debug('hostAvailable on connect setup')
            _this.setUp()
          } catch (ex) { }
        }
      }
    }).on('end', function () {
      this.debug && this.debug('hostAvailable on end')
    }).on('error', function (_err) {
      this.debug && this.debug("hostAvailable on error "+_err)
      _this.hostUnAvailable(host)
    }).on('timeout', function () {
      host.socket.destroy()
      this.debug && this.debug('hostAvailable timeout')
      this.timeOuts++
      _this.hostUnAvailable(host)
    }).setTimeout(100)
  }
  host.socket.connect(host.port, host.address || host.host)
}
module.exports = hostAvailable
