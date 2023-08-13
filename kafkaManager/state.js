/*
Copyright 2023 Jaroslav Peter Prib

setUpOnDownEmptyQ - when queuéd messages for down processed then set up
setIdleTime

*/

function State (node, logger = console) {
  if (node) {
    this.setMethods(node)
    this.node = node
  }
  this.logger = logger
  this.stack = { onUp: [], onDown: [], beforeUp: [], beforeDown: [], onError: [], setUpDone: [], setDownDone: [], onIdle: [] }
  this.resetDown()
  this.wait = { up: [], down: [] }
  this.maxQDepth = 1000
  this.upOnUpQDepth = null
  this.downOnDownQDepth = null
  this.downOnUpEmptyQ = false
  this.upOnDownEmptyQ = false
  this.idleTime = null
}
State.prototype.setIdleTime = function (time) {
  if (this.upActionCall == null) throw Error('no up action specified')
  this.idleTime = time
  this.onUp(this.setCheckIdleOn.bind(this))
  return this
}
State.prototype.setCheckIdleOff = function () {
  if (this.checkIdleTimer) {
    clearTimeout(this.checkIdleTimer)
    delete this.checkIdleTimer
  }
  return this
}
State.prototype.setCheckIdleOn = function () {
  if (!this.idleTime) return this
  const _this = this
  this.checkIdleTimer = setInterval(() => {
    try {
      if (this.lastCheckTime && this.lastUsed < this.lastCheckTime) {
        _this.setDown()
//        _this.callStack(this.stack.onIdle)
        this.callForEachNext(this.stack.onIdle, [], () => _this.setDownPart2())
      }
      _this.lastCheckTime = new Date()
    } catch (ex) {
      _this.logger.error(ex.message)
      _this.logger.error(ex.stack)
      _this.setCheckOff()
      _this.logger.error('*** check idle turned off***')
    }
  },
  this.idleTime
  )
  return this
}
State.prototype.beforeDown = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.beforeDown.push({ callFunction: callFunction, args: args })
  return this
}
State.prototype.beforeUp = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.beforeUp.push({ callFunction: callFunction, args: args })
  return this
}

State.prototype.callForEachNext = function (stack, args, done, index = 0) {
  if (stack.length <= index) {
    done && done()
    return
  }
  const action = stack[index]
  try {
    const _this = this
    action.callFunction(...action.args.concat(args, [() => _this.callForEachNext(stack, args, done, ++index)]))
  } catch (ex) {
    this.logger.error('stack call error: ' + ex.message)
    this.logger.error(ex.stack)
  }
}

State.prototype.callForEach = function (stack, ...args) {
  let error = 0
  for (const action of stack) {
    try {
      action.callFunction(...action.args.concat(args))
    } catch (ex) {
      error++
      this.logger.error('stack call error: ' + ex.message)
      this.logger.error(ex.stack)
    }
  }
  return error
}
State.prototype.callStack = function (stack, ...args) {
  let error = 0
  while (stack.length > 0) {
    try {
      const action = stack.shift()
      action.callFunction(...action.args.concat(args))
    } catch (ex) {
      error++
      this.logger.error('stack call error: ' + ex.message)
      this.logger.error(ex.stack)
    }
  }
  return error
}
State.prototype.clearWhenUpQ = function (callFunction, ...args) {
  if (callFunction) {
    for (const action of this.wait.up) {
      try {
        callFunction(...action.args.concat(args))
      } catch (ex) {
        this.logger.error('clearWhenUpQ error: ' + ex.message)
        this.logger.error(ex.stack)
      }
    }
  } else { this.wait.up = [] }
  if (this.upOnDownEmptyQ === true) this.setDown()
  return this
}
State.prototype.down = function (done) {
  this.logl1 && this.logl1.warn('down')
  if (this.available === false) {
    this.logl1 && this.logl1.warn('already down')
    return this
  }
  if (this.downActionCall && this.transitioning.down === false) throw Error('not transitioning down')
  else this.transitioning.down = true
  this.callForEach(this.stack.onDown)
  this.available = false
  this.callStack(this.stack.setDownDone)
  if (done) {
    this.done = done
    this.downWaitNext()
  } else {
    while (this.wait.down.length > 0 && this.available === false) {
      const action = this.wait.down.shift()
      action.callFunction(...action.args)
    }
    if (this.upOnDownEmptyQ === true) this.setUp()
  }
  return this
}
State.prototype.downWaitNext = function () {
  if (this.wait.down.length > 0) {
    const action = this.wait.down.shift()
    action.callFunction(...action.args, this.downWaitNext.bind(this))
  } else if (this.upOnDownEmptyQ === true) this.setUp()
  this.done()
}
State.prototype.error = function (err) {
  this.callForEach(this.stack.onError, err)
  return this
}
State.prototype.foundUp = function () {
  if (this.isDown()) this.setUp() // set status up
  return this
}
State.prototype.foundDown = function () {
  if (this.isAvailable()) this.setDown() // set status down
}
State.prototype.getDownQDepth = function () {
  return this.wait.down.length
}
State.prototype.getUpQDepth = function () {
  return this.wait.up.length
}
State.prototype.getState = function () {
  return {
    available: this.available,
    transitioning: { up: this.transitioning.up, down: this.transitioning.down },
    waiting: { up: this.wait.up.length, down: this.wait.down.length }
  }
}
State.prototype.isAvailable = function (up, down) {
  if (this.available === true) up && up()
  else down && down()
  return this.available
}
State.prototype.isNotAvailable = function (down, up) {
  if (this.available) up && up()
  else down && down()
  return !this.available
}
State.prototype.isDown = State.prototype.isNotAvailable
State.prototype.isTransitioning = function () {
  return this.transitioning.up === true || this.transitioning.down === true
}
State.prototype.isNotTransitioning = function () {
  return this.transitioning.up === false && this.transitioning.down === false
}
State.prototype.onError = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  if (callFunction) this.stack.onError.push({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onDown = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.onDown.push({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onIdle = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.onIdle.push({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onMaxQActionDefault = () => {
  throw Error('max queue size')
}
State.prototype.onMaxQUpAction = State.prototype.onMaxQActionDefault
State.prototype.onMaxQDownAction = State.prototype.onMaxQActionDefault
State.prototype.onUp = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  if (callFunction) this.stack.onUp.push({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onAvailable = State.prototype.up
State.prototype.onReady = State.prototype.up
State.prototype.onOpen = State.prototype.up
State.prototype.resetDown = function () {
  this.available = false
  this.transitioning = { up: false, down: false }
}
State.prototype.resetUp = function () {
  this.available = false
  this.transitioning = { up: false, down: false }
  return this
}
State.prototype.setCheckOff = function () {
  clearTimeout(this.checkTimer)
  return this
}
State.prototype.setCheck = function (callFunction, interval = 1000) {
  this.setCheckOn()
  if (callFunction == null) throw Error('no check function')
  this.checkCall = callFunction
  return this
}
State.prototype.setCheckOn = function (_this = this, foundUp = this.foundUp.bind(this), foundDown = this.foundDown.bind(this)) {
  this.checkTimer = setInterval(() => {
    try {
      _this.checkCall(foundUp, foundDown)
    } catch (ex) {
      this.logger.error(ex.message)
      this.logger.error(ex.stack)
      _this.setCheckOff()
      this.logger.error('*** check turned off***')
    }
  },
  _this.checkInterval
  )
  return this
}
State.prototype.setDown = function (done, ...args) {
  this.setCheckIdleOff()
  if (done) {
    this.stack.setDownDone.push({ callFunction: done, args: args })
  } else { this.testUp() }
  this.setDownTransitioning(done)
  const _this = this
  this.callForEachNext(this.stack.beforeDown, [], () => _this.setDownPart2())
  return this
}
State.prototype.setDownPart2 = function () {
  const down = this.down.bind(this)
  if (this.downActionCall) {
    this.downActionCall.callFunction(down, ...this.downActionCall.args)
  } else down()
  return this
}
State.prototype.setDownAction = function (callFunction, ...args) {
  if (!callFunction) throw Error('no function specified')
  this.downActionCall = { callFunction: callFunction, args: args }
  return this
}
State.prototype.setDownOnUpEmptyQ = function () {
  this.downOnUpEmptyQ = true
}
State.prototype.setDownOnDownQDepth = function (depth) {
  this.downOnDownQDepth = depth
}
State.prototype.setDownTransitioning = function (done) {
  if (this.transitioning.up) if (done) return this; else throw Error('transitioning up')
  if (this.transitioning.down) throw Error('transitioning down')
  this.transitioning.down = true
  return this
}
State.prototype.setOnMaxQUpAction = function (callFunction) {
  this.onMaxQUpAction = callFunction
}
State.prototype.setOnMaxDownAction = function (callFunction) {
  this.onMaxQDownAction = callFunction
}
State.prototype.setMethods = function (node) {
  if (node == null) throw Error('no object specified')
  node.available = this.up.bind(this)
  node.down = this.down.bind(this)
  node.getDownQDepth = this.getDownQDepth.bind(this)
  node.getUpQDepth = this.getUpQDepth.bind(this)
  node.getState = this.getState.bind(this)
  node.isAvailable = this.isAvailable.bind(this)
  node.isNotAvailable = this.isNotAvailable.bind(this)
  node.isDown = this.isDown.bind(this)
  node.isTransitioning = this.isTransitioning.bind(this)
  node.onUp = this.onUp.bind(this)
  node.onDown = this.onDown.bind(this)
  node.onIdle = this.onIdle.bind(this)
  node.onError = this.onError.bind(this)
  node.setError = this.error.bind(this)
  node.resetDown = this.resetDown.bind(this)
  node.resetUp = this.resetUp.bind(this)
  node.setDown = this.setDown.bind(this)
  node.setUp = this.setUp.bind(this)
  node.testConnected = this.testConnected.bind(this)
  node.testUp = this.testUp.bind(this)
  node.testDisconnected = this.testDisconnected.bind(this)
  node.testDown = this.testDown.bind(this)
  node.whenUp = this.whenUp.bind(this)
  node.whenDown = this.whenDown.bind(this)
  node.unavailable = this.down.bind(this)
  return this
}
State.prototype.setMaxQDepth = function (depth = 1000) {
  this.maxQDepth = depth
  return this
}
State.prototype.setUp = function (done, ...args) {
  if (done) {
    this.stack.setUpDone.push({ callFunction: done, args: args })
  } else { this.testDown() }
  this.setUpTransitioning(done)
  const _this = this
  this.callForEachNext(this.stack.beforeUp, [], () => _this.setUpPart2())
  return this
}
State.prototype.setUpPart2 = function () {
  const up = this.up.bind(this)
  if (this.upActionCall) {
    this.upActionCall.callFunction(up, ...this.upActionCall.args)
  } else up()
  return this
}
State.prototype.setUpAction = function (callFunction, ...args) {
  if (!callFunction) throw Error('no function specified')
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.upActionCall = { callFunction: callFunction, args: args }
  return this
}
State.prototype.setUpTransitioning = function (done) {
  if (this.transitioning.up) if (done) return this; else throw Error('transitioning up')
  if (this.transitioning.down) throw Error('transitioning down')
  this.transitioning.up = true
  return this
}
State.prototype.setUpOnDownEmptyQ = function () {
  this.upOnDownEmptyQ = true
  return this
}
State.prototype.setUpOnUpQDepth = function (depth) {
  this.upOnUpQDepth = depth
  return this
}
State.prototype.testConnected = function () {
  if (this.available === false) throw Error('unavailable')
  if (this.transitioning.down === true) throw Error('transitioning down')
  if (this.transitioning.up === true) throw Error('transitioning up')
  return this
}
State.prototype.testUp = State.prototype.testConnected
State.prototype.testDisconnected = function () {
  if (this.available === true) throw Error('available')
  if (this.transitioning.down === true) throw Error('transitioning down')
  if (this.transitioning.up === true) throw Error('transitioning up')
  return this
}
State.prototype.testDown = State.prototype.testDisconnected
State.prototype.up = function (done) {
  this.logl1 && this.logl1.s.logger.info('up')
  if (this.available === true) {
    this.logl1 && this.logl1.warn('already up')
    return this
  }
  if (this.upActionCall && this.transitioning.up === false) throw Error('not transitioning up')
  else this.transitioning.up = true
  this.callForEach(this.stack.onUp)
  this.transitioning.up = false
  this.available = true
  this.callStack(this.stack.setUpDone)
  if (done) {
    this.done = done
    this.upWaitNext()
  } else {
    while (this.wait.up.length > 0 && this.available === true) {
      this.lastUsed = new Date()
      const action = this.wait.up.shift()
      action.callFunction(...action.args)
    }
    if (this.downOnUpEmptyQ === true) this.setDown()
  }
  this.lastUsed = new Date()
  return this
}
State.prototype.upWaitNext = function () {
  if (this.wait.up.length > 0) {
    this.lastUsed = new Date()
    const action = this.wait.up.shift()
    action.callFunction(...action.args, this.upWaitNext)
  } else if (this.downOnUpEmptyQ === true) this.setDown()
  this.done()
}
State.prototype.upFailed = function (error) {
  this.callForEach(this.stack.onError, error)
  this.transitioning.up = false
  return this
}
State.prototype.upFailedandClearQ = function (error) {
  this.clearWhenUpQ()
  this.upFailed(error)
  return this
}
State.prototype.whenDown = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  const q = this.wait.down
  if (this.available === false && q.length === 0) {
    callFunction(...args)
  } else {
    if (this.maxQDepth < q.length) return this.onMaxQDownAction(callFunction, ...args)
    q.push({ callFunction: callFunction, args: args })
    if (this.downOnDownQDepth == null || this.downOnDownQDepth < q.length) return this
    if (this.downActionCall) this.setDown()
  }
  return this
}
State.prototype.whenUp = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  const q = this.wait.up
  if (this.available === true && q.length === 0) {
    this.lastUsed = new Date()
    callFunction(...args)
  } else {
    if (this.maxQDepth < q.length) return this.onMaxQUpAction(callFunction, args)
    q.push({ callFunction: callFunction, args: args })
    if (this.upActionCall) {
      try {
        if (this.upOnUpQDepth == null || q.length <= this.upOnUpQDepth ) return this
        this.setUp()
      } catch (ex) {
        if (ex.message === 'Disconnected') return this
        throw ex
      }
    }
  }
  return this
}
module.exports = State
