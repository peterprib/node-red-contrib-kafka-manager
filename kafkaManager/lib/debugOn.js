const ts = (new Date().toString()).split(' ')

Debug.prototype.sendConsole = function (m) {
  if (this.active) console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [debug] ' + this.label + ' ' + m)
}

Debug.prototype.set = function (count) {
  this.count = count
  this.active = false
  if (count > 0) this.active = true
  this.sendConsole(' debugging turning ' + this.active ? 'on' : 'off')
}

Debug.prototype.status = function () {
  return this.active
}

Debug.prototype.setOff = function () {
  this.set(0)
}

Debug.prototype.setOn = function (count) {
  this.set(count)
  this.sendConsole('debugging next ' + this.count + ' debug points')
}

Debug.prototype.debuglog = function (msg) {
  if (!this.count--) this.setOff()
  this.sendConsole(msg instanceof Object ? JSON.stringify(msg) : msg)
}

function Debug (label = '***', count = 100) {
  this.label = label
  this.set(count)
  console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [info] ' + label + ' Copyright 2020 Jaroslav Peter Prib')
}

module.exports = Debug
