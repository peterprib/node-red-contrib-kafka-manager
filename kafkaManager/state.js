/*
Copyright 2023 Jaroslav Peter Prib

setUpOnDownEmptyQ - when queuéd messages for down processed then set up
setIdleTime

*/
const { runInThisContext } = require('vm')
const ProcessStack = require('./processStack.js')

function State (node, logger = console) {
  const _this=this
  if (node) {
    this.setMethods(node)
    this.node = node
  }
  this.logger = logger
  this.stack = {
    onUp: this.getDoneStack(),
    onDown: this.getDoneStack(),
    beforeUp: this.getDoneStack(),
    beforeDown: this.getDoneStack(),
    onError: this.getDoneStack(),
    setUpDone: this.getDoneStack(),
    setDownDone: this.getDoneStack(),
    onIdle: this.getDoneStack()
  }
  this.resetDown()
  this.waitDown = this.getDoneStack()
  this.waitDown.setNextShift().setMaxDepth();
  this.waitUp = this.getDoneStack()
  this.waitUp.setRunnableTest(this.isAvailable.bind(this))
  this.waitUp.setNextShift().setMaxDepth()
  .setActionOnMax(()=>_this.isAvailable(_this.runWaitUp.bind(this),_this.setUp.bind(this)))
  this.setMaxQDepth();
  this.downOnUpEmptyQ = false
  this.upOnDownEmptyQ = false
  this.idleTime = null
}
State.prototype.getDoneStack = function () {
  const stack=new ProcessStack()
  return stack.setNext()
}
State.prototype.setDownOnDownQDepth = function (depth) {
  this.waitDown.setMaxDepth(depth)
  return this
}
State.prototype.setUpOnUpQDepth = function (depth) {
  this.waitUp.setMaxDepth(depth)
  return this
}
State.prototype.beforeDown = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.beforeDown.add({ callFunction: callFunction, args: args })
  return this
}
State.prototype.beforeUp = function (callFunction, ...args) {
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.beforeUp.add({ callFunction: callFunction, args: args })
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
    this.logger.error('stack call error: ' + ex.message + " stack index: " + index + " value: "+ + JSON.stringify(action))
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
State.prototype.clearWhenUpQ = function (done=(next)=>next&&next(),reason, callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, clearWhenUpQ')
  this.waitUp.clearQ(()=>{
      this.logl1 && this.logl1.warn('state, clearWhenUpQ done')
      if (this.upOnDownEmptyQ === true) this.setDown(()=>done()) 
      else done()
    },
    (next,action)=>{
      try {
        this.logl1 && this.logl1.warn('state, clearWhenUpQ action onDisgard:'+action.onDisgard)
        if (action.onDisgard) action.onDisgard(reason,
          ()=>{
            if (callFunction) callFunction(next,...action.args.concat(args))
            else next()
          },
          ...action.args.concat(args))
        if (callFunction) callFunction(next,...action.args.concat(args))
        else  next()
      } catch (ex) {
        this.logger.error('clearWhenUpQ error: ' + ex.message)
        this.logger.error(ex.stack)
      }
    }
  )
  return this
}
State.prototype.down = function (done) {
  this.logl1 && this.logl1.warn('state, down')
  if (this.available === false) {
    this.logl1 && this.logl1.warn('state, already down')
    return this
  }
  if (this.downActionCall && this.transitioning.down === false) throw Error('not transitioning down')
  else this.transitioning.down = true
  this.stack.onDown.run()
  this.forceDown(done)
}
State.prototype.downWaitNext = function () {
  if (this.wait.down.length > 0) {
    const action = this.wait.down.shift()
    action.callFunction(...action.args, this.downWaitNext.bind(this))
  } else if (this.upOnDownEmptyQ === true) this.setUp()
  this.done()
}
State.prototype.error = function (err) {
  this.stack.onError.run(err)
  return this
}
State.prototype.foundUp = function () {
  if (this.isDown()) this.setUp() // set status up
  return this
}
State.prototype.foundDown = function () {
  if (this.isAvailable()) this.setDown() // set status down
}
State.prototype.forceDown = function (done) {
  this.logl1 && this.logl1.warn('state, forceDown')
  this.transitioning.up = false
  if ( this.transitioning.down !== true && this.available === false) {
    done && done()
    return this
  } 
  this.transitioning.down = true  
  this.available = false
  const _this = this
  this.stack.setDownDone.run(()=>{
    this.transitioning.down = false
    _this.waitDown.run(()=>{
      if (_this.upOnDownEmptyQ === true) _this.setDown(done)
      else done && done()
    })
  })
  return this
}
State.prototype.getDownQDepth = function () {
  return this.wait.down.length
}
State.prototype.getUpQDepth = function () {
  return this.waitUp.length
}
State.prototype.getState = function () {
  return {
    available: this.available,
    transitioning: { up: this.transitioning.up, down: this.transitioning.down },
    waiting: { up: this.waitUp.getDepth(), down: this.waitDown.getDepth() }
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
  return this.available === false || this.transitioning.up === true || this.transitioning.down === true
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
  if (callFunction) this.stack.onError.add({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onDown = function (callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, onDown')
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.onDown.add({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onIdle = function (callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, onIdle')
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.stack.onIdle.add({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onMaxQActionDefault = () => {
  throw Error('max queue size')
}
State.prototype.onMaxQUpAction = State.prototype.onMaxQActionDefault
State.prototype.onMaxQDownAction = State.prototype.onMaxQActionDefault
State.prototype.onUp = function (callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, onUp')
  if (typeof callFunction !== 'function') throw Error('expected function')
  if (callFunction) this.stack.onUp.add({ callFunction: callFunction, args: args })
  return this
}
State.prototype.onAvailable = State.prototype.up
State.prototype.onReady = State.prototype.up
State.prototype.onOpen = State.prototype.up
State.prototype.resetDown = function () {
  this.logl1 && this.logl1.warn('state, resetDown')
  this.available = false
  this.transitioning = { up: false, down: false }
}
State.prototype.resetUp = function () {
  this.logl1 && this.logl1.warn('state, resetUp')
  this.available = false
  this.transitioning = { up: false, down: false }
  return this
}
State.prototype.runWaitUp = function (done) {
  this.logl1 && this.logl1.info('state, runWaitUp')
  const _this = this
  this.waitUp.run(()=>{
    _this.logl1 && _this.logl1.info('state, runWaitUp finished')
    _this.lastUsed = new Date()
    if (_this.downOnUpEmptyQ === true) {
      _this.logl1 && this.logl1.info('state, runWaitUp downOnUpEmptyQ')
      _this.setDown()
    }
    done && done()
  })
}
State.prototype.setCheck = function (callFunction, interval = 1000) {
  this.logl1 && this.logl1.warn('state, SetCheck')
  this.checkInterval = interval
  if(this.checkTimer){
    clearInterval(this.checkTimer)
  } delete this.checkTimer
  this.setCheckOn()
  if (callFunction == null) throw Error('no check function')
  this.checkCall = callFunction
  return this
}
State.prototype.setCheckOff = function () {
  this.logl1 && this.logl1.info('state, setCheckOff')
  clearTimeout(this.checkTimer)
  return this
}
State.prototype.setCheckOn = function (_this = this, foundUp = this.foundUp.bind(this), foundDown = this.foundDown.bind(this)) {
  this.logl1 && this.logl1.warn('state, setCheckOn')
  if(this.checkIdleTimer) return this
  this.checkTimer = setInterval(() => {
    _this.logl1 && this.logl1.warn('state, setCheckOn check')
    try {
        _this.checkCall(foundUp, foundDown)
      } catch (ex) {
        _this.logger.error(ex.message)
        _this.logger.error(ex.stack)
        _this.setCheckOff()
        _this.logger.error('*** check turned off***')
      }
    },
    this.checkInterval
  )
  return this
}
State.prototype.setCheckIdleOff = function () {
  this.logl1 && this.logl1.warn('state, setCheckIdleOff')
  if (this.checkIdleTimer) {
    clearTimeout(this.checkIdleTimer)
    delete this.checkIdleTimer
  } else {
    this.logl1 && this.logl1.warn('state, setCheckIdleOff timer already cleared')
  }
  return this
}
State.prototype.setCheckIdleOn = function (done) {
  this.logl1 && this.logl1.warn('state, setCheckIdleOn')
  if(this.checkIdleTimer) throw Error("idle check already running")
  if (!this.idleTime) return this
  const _this = this
  this.checkIdleTimer = setInterval(() => {
      try {
        _this.logl1 && _this.logl1.info('state, idle check ')
        _this.lastCheckTime = new Date()
        if (_this.lastCheckTime && _this.lastUsed < _this.lastCheckTime || _this.lastUsed == null) {
          _this.testUp(
            ()=>{    // up
              _this.logl1 && this.logl1.info('state, onIdle set whenDown')
              _this.whenDown((next)=>{
                _this.logl1 && this.logl1.info('state, onIdle run')
                _this.stack.onIdle.run(() =>{ 
                  _this.logl1 && this.logl1.info('state, onIdle run finish')
                  next()
                })
              })
              _this.setDown()
            },
            ()=> { // down
              _this.logl1 && this.logl1.info('state, onIdle down')
              _this.setCheckIdleOff()
            }, 
            ()=> { // going down
              _this.logl1 && this.logl1.info('state, onIdle going down')
              _this.setCheckIdelOff()
            },
            ()=> _this.logl1 && this.logl1.info('state, idle check is transitioning up')   // going up
          )
        }
      } catch (ex) {
        _this.logger.error(ex.message)
       _this.logger.error(ex.stack)
       _this.setCheckIdleOff()
       _this.logger.error('*** check idle turned off***')
      }
    },
    this.idleTime
  )
  done && done()
  return this
}
State.prototype.setDown = function (done, ...args) {
  this.logl1 && this.logl1.info('state, setDown')
  this.setCheckIdleOff()
  if (done) {
    this.stack.setDownDone.add({ callFunction: done, args: args })
  } else { this.testUp() }
  this.setDownTransitioning(done)
  const _this = this
  this.logl1 && this.logl1.info('state, beforeDown')
  this.stack.beforeDown.run(() => _this.setDownPart2())
  return this
}
State.prototype.setDownPart2 = function () {
  this.logl1 && this.logl1.info('state, setDownPart2')
  const down = this.down.bind(this)
  if (this.downActionCall) {
    this.logl1 && this.logl1.info('state, downActionCall')
    this.downActionCall.callFunction(down, ...this.downActionCall.args)
  } else down()
  return this
}
State.prototype.setDownAction = function (callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, setDownAction')
  if (!callFunction) throw Error('no function specified')
  this.downActionCall = { callFunction: callFunction, args: args }
  return this
}
State.prototype.setDownOnUpEmptyQ = function () {
  this.logl1 && this.logl1.warn('state, setDownOnUpEmptyQ')
  this.downOnUpEmptyQ = true
}
State.prototype.setDownTransitioning = function (done) {
  this.logl1 && this.logl1.warn('state, setDownTransitioning')
  if (this.transitioning.up) if (done) return this; else throw Error('transitioning up')
  if (this.transitioning.down) throw Error('transitioning down')
  this.transitioning.down = true
  return this
}
State.prototype.setIdleTime = function (time) {
  this.logl1 && this.logl1.info('state, set idle time '+time)
  if (this.upActionCall == null) throw Error('no up action specified')
  this.idleTime = time
  this.onUp(this.setCheckIdleOn.bind(this))
  return this
}
State.prototype.setLogLevel1 = function (callFunction=console) {
  this.logl1 = callFunction
  return this
}
State.prototype.setOnMaxQUpAction = function (callFunction) {
  this.logl1 && this.logl1.warn('state, setOnMaxQUpAction')
  this.onMaxQUpAction = callFunction
  return this
}
State.prototype.setOnMaxDownAction = function (callFunction) {
  this.logl1 && this.logl1.warn('state, setOnMaxDownAction')
  this.onMaxQDownAction = callFunction
  return this
}
State.prototype.setMaxQDepth = function (depth = 1000) {
  this.logl1 && this.logl1.warn('state, setMaxQDepth')
  this.waitUp.setMaxDepth(depth)
  this.waitDown.setMaxDepth(depth)
  return this
}
State.prototype.setUp = function (done, ...args) {
  this.logl1 && this.logl1.info('state, setUp')
  if (done) {
    this.stack.setUpDone.add({ callFunction: done, args: args })
  }
  if( this.transitioning.up === true) {
    this.logl1 && this.logl1.info('state, setUp already transitioning up')
    return this
  }
  this.testDown() 
  this.setUpTransitioning()
  const _this = this
  this.logl1 && this.logl1.info('state, beforeUp')
  this.stack.beforeUp.run(() => _this.setUpPart2())
  return this
}
State.prototype.setUpPart2 = function () {
  this.logl1 && this.logl1.info('state, setUpPart2')
  const up = this.up.bind(this)
  if (this.upActionCall) {
    this.upActionCall.callFunction(up,this.forceDown.bind(this), ...this.upActionCall.args)
  } else up()
  return this
}
State.prototype.setUpAction = function (callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, setUpAction')
  if (!callFunction) throw Error('no function specified')
  if (typeof callFunction !== 'function') throw Error('expected function')
  this.upActionCall = { callFunction: callFunction, args: args }
  return this
}
State.prototype.setUpTransitioning = function (done) {
  this.logl1 && this.logl1.warn('state, setUpTransitioning')
  if (this.transitioning.up) if (done) return this; else throw Error('aready transitioning up')
  if (this.transitioning.down) throw Error('transitioning down')
  this.transitioning.up = true
  return this
}
State.prototype.setUpOnDownEmptyQ = function () {
  this.logl1 && this.logl1.warn('state, setUpOnDownEmptyQ')
  this.upOnDownEmptyQ = true
  return this
}
State.prototype.testConnected = function (
    ok=()=>this,
    onUnavailable=()=>{throw Error('unavailable')},
    onTransitioningDown=()=>{throw Error('transitioning down')},
    onTransitioningUp=()=>{throw Error('transitioning up')
  }) {
  this.logl1 && this.logl1.warn('state, testConnected')
  if (this.available === false) onUnavailable()
  else if (this.transitioning.down === true) onTransitioningDown()
  else if (this.transitioning.up === true) onTransitioningUp()
  else ok()
  return this
}
State.prototype.testUp = State.prototype.testConnected
State.prototype.testDisconnected = function (
    ok=()=>this,
    onAvailable=()=>{throw Error('available')},
    onTransitioningDown=()=>{throw Error('transitioning down')},
    onTransitioningUp=()=>{throw Error('transitioning up')}
  ) {
  this.logl1 && this.logl1.warn('state, tesDisconnected')
  if (this.available === true) onAvailable()
  else if (this.transitioning.down === true) onTransitioningDown()
  else if (this.transitioning.up === true) onTransitioningUp()
  else ok()
  return this
}
State.prototype.testDown = State.prototype.testDisconnected
State.prototype.up = function (done) {
  this.logl1 && this.logl1.info('state, up')
  if (this.available === true) {
    this.logl1 && this.logl1.warn('state, already up')
    return this
  }
//  if(this.transitioning.up === false) {
//    if (this.upActionCall) throw Error('not transitioning up')
//    else  this.transitioning.up = true
//  }
  this.transitioning.up = true
  this.logl1 && this.logl1.info('state, onUp run')
  const _this = this
  this.stack.onUp.run(()=>{
    this.logl1 && this.logl1.info('state, onUp run finished')
    _this.transitioning.up = false
    _this.available = true
    _this.logl1 && this.logl1.info('state, setUpDone')
    _this.stack.setUpDone.run(()=>{
      this.logl1 && this.logl1.info('state, setUpDone finish')
      _this.runWaitUp(done)
    })  
  })
  return this
}
State.prototype.upFailed = function (error,done) {
  this.logl1 && this.logl1.info('state, upFailed')
  this.stack.onError.run(()=>{
    this.transitioning.up = false
    done&&done()
  },error)
  return this
}
State.prototype.upFailedAndClearQ = function (error,done) {
  this.logl1 && this.logl1.warn('state, upFailedAndClearQ')
  this.clearWhenUpQ(()=>{
      this.upFailed(error,done)
      done&&done()
    },
    error
  )
  return this
}

State.prototype.whenDown = function (callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, whenDown')
  if(this.transitioning.up===true) {
    this.logl1 && this.logl1.warn('state, whenDown transitioning down so run when up')
    const _this=this
    this.whenUp((next)=>{
      this.logl1 && this.logl1.warn('state, whenDown transitioning down, now down so adding as whenDown')
      _this.waitDown.add2Stack(callFunction, ...args)
      next()
    })
  } else this.waitDown.add(callFunction, ...args)
  return this
}

State.prototype.whenUp = function (callFunction, ...args) {
  this.logl1 && this.logl1.warn('state, whenUp')
  if(this.transitioning.down===true) {
    this.logl1 && this.logl1.warn('state, whenUp transitioning down so run when down')
    const _this=this
    this.whenDown((next)=>{
      this.logl1 && this.logl1.warn('state, whenUp transitioning down, now down so adding as whenup')
      _this.waitUp.add2Stack(callFunction, ...args)
      next()
    })
  } else
    this.waitUp.add(callFunction, ...args)
  return this
}

State.prototype.setMethods = function (node) {
  if (node == null) throw Error('no object specified')
  node.available = this.up.bind(this)
  node.forceDown = this.foundDown.bind(this)
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
  node.setIdleTime = this.setIdleTime.bind(this)
  node.setUp = this.setUp.bind(this)
 // node.setUpOnUpQDepth = this.setUpOnUpQDepth.bind(this)
  node.testConnected = this.testConnected.bind(this)
  node.testUp = this.testUp.bind(this)
  node.testDisconnected = this.testDisconnected.bind(this)
  node.testDown = this.testDown.bind(this)
  node.upFailedAndClearQ = this.upFailedAndClearQ.bind(this)
  node.whenUp = this.whenUp.bind(this)
  node.whenDown = this.whenDown.bind(this)
  node.unavailable = this.down.bind(this)
  return this
}

module.exports = State
