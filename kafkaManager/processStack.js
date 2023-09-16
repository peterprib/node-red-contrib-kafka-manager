const nextTick = require('process').nextTick

class ProcesStack {
  constructor (onError) {
    this.stack = []
    this.onError = onError
    this.run = this.runSequentially
    this.stopOnError = true
    this.maxDepth = 1000
    this.runOnDepth = null
    this.runOnEmpty = null
    this.concurrent = 11111
    return this
  }

  add (callFunction, ...args) {
    if (typeof callFunction === 'object') {
      this.stack.push({ callFunction: callFunction, args: args })
      return this
    }
    if (typeof callFunction !== 'function') throw Error('expected function')
    this.stack.push({ callFunction: callFunction, args: args })
    return this
  }

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

  setConcurrent (concurrent) {
    this.concurrent = concurrent
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
    const workarea = { completedCount: 0, done: done, args: args, errors: [], concurrent: 0}
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

  runSequentiallyShift (done, ...args) {
    const errors = []
    while (this.stack) {
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

  setRunNext () { this.run = this.runNext; return this }

  setRunSequentially () { this.run = this.runSequentially; return this }
}
module.exports = ProcesStack
