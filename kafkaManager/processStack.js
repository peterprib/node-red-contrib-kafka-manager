const nextTick = require('process').nextTick

class ProcesStack {
  constructor (onError) {
    this.push=this.add.bind(this)
    this.stack = []
    this.onError = onError
    this.runAction = this.runSequentially
    this.running = false
    this.isRunnable=()=>true
    this.stopOnError = true
    this.maxDepth = 1000
    this.ActionOnEmpty = null
    this.concurrent = 11111
    this.maxWait = null
    this.destructiveStack=false
    this.setActionOnMax()
    return this
  }

  add (callFunction, ...args) {
    this.add2Stack(callFunction,...args)
    if (this.running === true) return this
    if (this.stack.length > this.maxDepth) {
      this.actionOnMax && this.actionOnMax()
    } else {
      this.setWaitOn()
    }
    return this
  }
  add2Stack (callFunction, ...args) {
    if (typeof callFunction === 'object') {
      if(!callFunction.callFunction) throw Error("no callFunction property in "+JSON.stringify(callFunction))
      if(callFunction.args) callFunction.args = callFunction.args = callFunction.args.concat(args)
      else callFunction.args = args
      this.stack.push(callFunction) 
    } else {
      if (typeof callFunction !== 'function') throw Error('expected function')
      this.stack.push({ callFunction: callFunction, args: args })
    }
    return this
  }

  addDisgard () { this.stack.pop() }

  addDisgardFirst () { this.stack.shift() }

  callForEachNext (workarea, done, onError, index = 0) {
    if (this.stack.length <= index) {
      done && done()
      return this
    }
    const action = this.stack[index]
    try {
      const _this = this
      action.callFunction(() => _this.callForEachNext(workarea, done, onError, ++index), ...action.args.concat(workarea.args))
    } catch (ex) {
      if (action.onError) action.onError(ex)
      if (onError) onError(ex)
    }
  }

clearQ (done,callFunction,...args) {
    if(callFunction) {
      this.clearQNextShift (done, callFunction, ...args)
    } else {
      this.stack = []
      done();
    }
    return this
}

clearQNextShift (done, callFunction, ...args) {
    const _this = this
    this.clearQNextFramework(() => _this.stack.length <= 0, () => _this.stack.shift(),done, callFunction, ...args)
  }

clearQNextFramework (hasFinished, getNext, done, callFunction, ...args) {
    if (hasFinished()) {
      done && done()
      return this
    }
    const action = getNext()
    try {
      const _this = this
      callFunction(() => _this.clearQNextFramework(hasFinished, getNext, done, callFunction, ...args), action ,...action.args.concat(...args))
    } catch (ex) {
      if (action.onError) {
        action.onError(ex)
      } else console.error(ex)
      this.clearQNextFramework(hasFinished, getNext, done, ...args)
    }
    return this
  }

  getDepth () {
    return this.stack.length
  }

  run (done=(next)=>next&&next(), ...args) {
    this.running = true
    this.runAction(done, ...args)
    this.running = false
    return this
  }

  runNext (done, ...args) {
    const workArea = { completedCount: 0, done: done, args: args, concurrent: 0 }
    this.callForEachNext(workArea, done)
    return this
  }

  runNextPop (done, ...args) {
    const _this = this
    this.runNextFramework(() => _this.stack.length <= 0, () => _this.stack.pop(), done, ...args)
  }

  runNextShift (done, ...args) {
    const _this = this
    this.runNextFramework(() => _this.stack.length <= 0, () => _this.stack.shift(), done, ...args)
  }

  runNextFramework (hasFinished, getNext, done, ...args) {
    if (hasFinished()) {
      if (this.actionOnEmpty) this.actionOnEmpty(done)
      else done && done()
      return this
    }
    const action = getNext()
    try {
      const _this = this
      action.callFunction(() => _this.runNextFramework(hasFinished, getNext, done, ...args), ...action.args.concat(...args))
    } catch (ex) {
      if (action.onError) {
        action.onError(ex)
      } else console.error(ex)
      this.runNextFramework(hasFinished, getNext, done, ...args)
    }
    return this
  }

  runAsync (done, ...args) {
    if (done == null) throw Error('done function required')
    if (typeof done !== 'function') throw Error('Done is not a function')
    const workarea = { completedCount: 0, done: done, args: args, errors: [], concurrent: 0, index: 0 }
    this.runAsyncWaiting(workarea)
    workarea.concurrent++
    this.runAsyncEnd(workarea)
    return this
  }

  runAsyncShift (done, ...args) {
    if (done == null) throw Error('done function required')
    if (typeof done !== 'function') throw Error('Done is not a function')
    const workarea = { completedCount: 0, done: done, args: args, errors: [], concurrent: 0 }
    this.runAsyncWaitingShift(workarea)
    workarea.concurrent++
    this.runAsyncEndShift(workarea)
    return this
  }

  runAsyncWaiting (workarea) {
    const _this = this
    while (workarea.index < this.stack.length) {
      try {
        if (workarea.concurrent > this.concurrent) return
        const action = this.stack[workarea.index]
        workarea.index++
        workarea.concurrent++
        nextTick(
          action.callFunction,
          () => _this.runAsyncEnd(workarea), ...action.args.concat(workarea.args)
        )
      } catch (ex) {
        this.runAsyncEnd(workarea, ex)
      }
    }
  }

  runAsyncWaitingShift (workarea) {
    const _this = this
    while (this.stack.length) {
      try {
        if (workarea.concurrent > this.concurrent) return
        const action = this.stack.shift()
        workarea.concurrent++
        nextTick(
          action.callFunction,
          () => _this.runAsyncEndShift(workarea), ...action.args.concat(workarea.args)
        )
      } catch (ex) {
        this.runAsyncEndShift(workarea, ex)
      }
    }
  }

  runAsyncEnd (workarea, error) {
    workarea.concurrent--
    workarea.completedCount++
    if (error) workarea.push(error)
    if (workarea.completedCount <= this.stack.length) {
      this.runAsyncWaiting(workarea)
      return
    }
    if (workarea.concurrent) return
    workarea.done && workarea.done()
  }

  runAsyncEndShift (workarea, error) {
    workarea.concurrent--
    workarea.completedCount++
    if (error) workarea.push(error)
    if (this.stack.length > 0) {
      this.runAsyncWaitingShift(workarea)
      return
    }
    if (workarea.concurrent) return
    workarea.done && workarea.done()
  }

  runSequentially (done, ...args) {
    const errors = []
    for (const action of this.stack) {
      try {
        action.callFunction(...action.args.concat(args))
      } catch (ex) {
        errors.push(ex)
        action.onError && action.onError(ex)
        if (this.stopOnError || action.stopOnError) throw ex
      }
    }
    if (done) done(errors.length > 0 ? errors : undefined)
    else if (errors.length > 0) throw errors
    return this
  }

  runSequentiallyPop (done, ...args) {
    const errors = []
    while (this.stack.length) {
      const action = this.stack.pop()
      try {
        action.callFunction(...action.args.concat(args))
      } catch (ex) {
        errors.push(ex)
        action.onError && action.onError(ex)
        if (this.stopOnError || action.stopOnError) throw ex
      }
    }
    if (done) done(errors.length > 0 ? errors : undefined)
    else if (errors.length > 0) throw errors
    return this
  }

  runSequentiallyShift (done, ...args) {
    const errors = []
    while (this.stack.length) {
      const action = this.stack.shift()
      try {
        action.callFunction(...action.args.concat(args))
      } catch (ex) {
        errors.push(ex)
        action.onError && action.onError(ex)
        if (this.stopOnError || action.stopOnError) throw ex
      }
    }
    if (done) done(errors.length > 0 ? errors : undefined)
    else if (errors.length > 0) throw errors
    return this
  }

  setActionOnMax (callFunction=this.run.bind(this)) {
    this.actionOnMax = callFunction
    return this
  }

  setActionOnEmpty (callFunction) {
    this.ActionOnEmpty = callFunction
    return this
  }

  setAsync () { this.run = this.runAsync; return this }

  setAsyncShift () {
    this.destructiveStack=true
    this.run = this.runAsyncShift
     return this
  }

  setConcurrent (concurrent) {
    this.concurrent = concurrent
    return this
  }

  setMaxWait (maxWait) { this.maxWait = maxWait; return this }

  setMaxDepth (maxDepth) {
    if(this.destructiveStack===true){
      this.maxDepth = maxDepth
    } else throw Error('not destructive stack')
    if (!this.maxWait) this.setMaxWait(1000)
    return this
  }

  setNext () { this.runAction = this.runNext; return this }

  setNextPop () {
    this.destructiveStack=true
    this.runAction = this.runNextPop
    return this
  }

  setNextShift () {
    this.destructiveStack=true
    this.runAction = this.runNextShift
    return this
  }

  setRunnableTest (callFunction) { this.isRunnable = callFunction; return this }
  
  setSequentially () { this.runAction = this.runSequentially; return this }

  setSequentiallyShift () {
    this.destructiveStack=true
    this.runAction = this.runSequentiallyShift
    return this
  }

  setSequentiallyPop () {
    this.destructiveStack=true
    this.runAction = this.runSequentiallyPop
    return this
  }

  setWaitOff () {
    if (this.waitTimer) {
      clearTimeout(this.waitTimer)
      delete this.waitTimer
    }
    return this
  }

  setWaitOn () {
    if (this.waitTimer != null || this.maxWait === null) return this
    const _this = this
    this.waitTimer = setInterval(() => {
      try {
        if(!_this.isRunnable()) return
        _this.run()
      } catch (ex) {
        console.error(ex)
      }
      this.setWaitOff()
    },
    this.maxWait
    )
    return this
  }
}

module.exports = ProcesStack
