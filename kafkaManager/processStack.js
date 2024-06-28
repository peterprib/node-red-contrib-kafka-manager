const nextTick = require('process').nextTick
const processtimeOut=(timeOut=1000,done,action)=>setTimeout(() => {
    console.error("processStack timeOut "+timeOut/1000+" secs"+(action&&action.labe?" label: " +action.label:"") );
    action&&console.error("function:"+action.callFunction.toString())
    done&&done()
  }, timeOut);
class ProcesStack {
  constructor (onError) {
    this.debugFunction=console
    this.timeOut=60000
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
    this.debug&&this.debug.log("add")
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
    this.debug&&this.debug.log("add2Stack")
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

  addDisgard () { 
    this.debug&&this.debug.log("addDisgard")
    if(this.stack.length) this.stack.pop() 
  }

  addDisgardFirst () { 
    this.debug&&this.debug.log("addDisgardFirst")
    if(this.stack.length) this.stack.shift() 
  }

  callForEachNext (workarea, done, onError, index = 0) {
    this.debug&&this.debug.log("callForEachNext index: "+index)
    workarea.timeOut&&clearTimeout(workarea.timeOut)
    if (this.stack.length <= index) {
      if(this.stack.length < index){
        onError&&onError("processStack trying to process greater than stack size")
        throw Error("processStack trying to process greater than stack size")
      }
      done && done()
      return this
    }
    const action = this.stack[index]
    try {
      const _this = this
      const next=function() {_this.callForEachNext(workarea, done, onError, ++index)}
      if(this.timeOut) workarea.timeOut=processtimeOut(this.timeOut,next,action)
      action.callFunction(next, ...action.args.concat(workarea.args))
    } catch (ex) {
      console.error(ex.stack)
      if (action.onError) action.onError(ex,t(workarea, done, onError, ++index))
      else if (onError){
        onError(ex,()=>this.callForEachNext(workarea, done, onError, ++index))
      } else {
        this.callForEachNext(workarea, done, onError, ++index)
      }
    }
  }

  clearQ (done,callFunction,...args) {
    this.debug&&this.debug.log("clearQ")
    if(callFunction) {
      this.clearQNextShift (done, callFunction, ...args)
    } else {
      this.stack = []
      done();
    }
    return this
  }

  clearQNextShift (done, callFunction, ...args) {
    this.debug&&this.debug.log("clearQNextShift")
    const _this = this
    this.clearQNextFramework(() => _this.stack.length <= 0, () => _this.stack.shift(),done, callFunction, ...args)
  }

  clearQNextFramework (hasFinished, getNext, done, callFunction, ...args) {
    this.debug&&this.debug.log("clearQNextFramework")
    if (hasFinished()) {
      done && done()
      return this
    }
    const action = getNext()
    try {
      const _this = this
      callFunction(() => _this.clearQNextFramework(hasFinished, getNext, done, callFunction, ...args), action ,...action.args.concat(...args))
    } catch (ex) {
      console.error(ex.stack)
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

  run (done=next=>next&&next(), ...args) {
    this.debug&&this.debug.log("run")
    this.running = true
    this.runAction(done, ...args)
    this.running = false
    return this
  }

  runNext (done, ...args) {
    this.debug&&this.debug.log("runNext")
    const workArea = { completedCount: 0, done: done, args: args, concurrent: 0}
    this.callForEachNext(workArea, done)
    return this
  }

  runNextPop (done, ...args) {
    this.debug&&this.debug.log("runNextPop")
    const _this = this
    this.runNextFramework(() => _this.stack.length <= 0, () => _this.stack.pop(), done, ...args)
  }

  runNextShift (done, ...args) {
    this.debug&&this.debug.log("runNextShift")
    const _this = this
    this.runNextFramework(() => _this.stack.length <= 0, () => _this.stack.shift(), done, ...args)
  }

  runNextFramework (hasFinished, getNext, done, ...args) {
    this.debug&&this.debug.log("runNextFramework hasFinished: ",hasFinished())
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
      console.error(ex.stack)
      if (action.onError) {
        action.onError(ex,_this.runNextFramework(hasFinished, getNext, done, ...args))
      } else console.error(ex)
      this.runNextFramework(hasFinished, getNext, done, ...args)
    }
    return this
  }
  getAsyncWorkarea(properties){
    const workArea = Object.assign({ completedCount: 0, errors: [], concurrent: 0, index: 0, promises:[] },properties)
    this.asyncCallWorkarea(workArea)
    return workArea
  }
  runAsync (done, ...args) {
    this.debug&&this.debug.log("runAsync")
    if (done == null) throw Error('done function required')
    if (typeof done !== 'function') throw Error('Done is not a function')
    const _this=this
    const workarea = this.getAsyncWorkarea({index:0,done: done, args: args,
      getNext: ()=>_this.stack[workarea.index++],
      runEnd:this.runAsyncEnd.bind(this),
      runNext:this.runAsyncWaiting.bind(this),
      isNotCompleted:()=>workarea.completedCount <= _this.stack.length,
      isThereMoreToRun: ()=>workarea.index < _this.stack.length
    })
    workarea.concurrent++
    this.runAsyncEnd(workarea)
    return this
  }

  runAsyncShift (done, ...args) {
    this.debug&&this.debug.log("runAsyncShift")
    if (done == null) throw Error('done function required')
    if (typeof done !== 'function') throw Error('Done is not a function')
    const _this=this
    const workarea = this.getAsyncWorkarea({done: done, args: args,
      getNext: ()=>_this.stack.shift(),
      runAsyncEnd:_this.runAsyncEnd.bind(this),
      runNext:_this.runAsyncWaiting.bind(this),
      isNotCompleted:()=>_this.stack.length,
      isThereMoreToRun:()=>_this.stack.length
    })
    workarea.concurrent++
    this.runAsyncEnd(workarea)
    return this
  }

  runAsyncWaiting (workarea) {
    this.debug&&this.debug.log("runAsyncWaiting")
    const _this = this
    while (workarea.isThereMoreToRun()) {
      try {
        if (workarea.concurrent > this.concurrent) return
        const action=workarea.getNext();
        const asyncCall=async () =>{
          try{
            action.callFunction(...action.args.concat(workarea.args))
          } catch(ex) {
            log.error(ex.stack)
          }
        }
        workarea.promises.push(asyncCall())
        workarea.concurrent++
      } catch (ex) {
        this.debug&&this.debug.log("runAsyncWaiting error: "+ex.message)
        this.debug&&console.error(ex.stack)
        this.runAsyncEnd(workarea, ex)
      }
    }
  }
  async asyncCallWorkarea(workarea) {
    this.debug&&this.debug.log('asyncCallWorkarea')
    try{
      Promise.any(workarea.promises)
      .then(() =>{
        _this.debug&&_this.debug.log('asyncCallWorkarea promise any')
        _this.runAsyncEnd(workarea)
      });
    } catch(ex) {
      log.error(ex.stack)
    }
  }
  
  runAsyncEnd (workarea, ex) {
    this.debug&&this.debug.log("runAsyncEnd")
    workarea.concurrent--
    workarea.completedCount++
    if (ex) workarea.errors.push(ex)
    if (workarea.isNotCompleted()) {
      workarea.runNext(workarea)
      return
    }
    if (workarea.concurrent) return
    workarea.done && workarea.done()
  }

  runSequentially (done, ...args) {
    this.debug&&this.debug.log("runSequentially")
    const errors = []
    for (const action of this.stack) {
      try {
        action.callFunction(...action.args.concat(args))
      } catch (ex) {
        console.error(ex.stack)
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
    this.debug&&this.debug.log("runSequentiallyPop")
    const errors = []
    while (this.stack.length) {
      const action = this.stack.pop()
      try {
        action.callFunction(...action.args.concat(args))
      } catch (ex) {
        console.error(ex.stack)
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
    this.debug&&this.debug.log("runSequentiallyShift")
    const errors = []
    while (this.stack.length) {
      const action = this.stack.shift()
      try {
        action.callFunction(...action.args.concat(args))
      } catch (ex) {
        console.error(ex.stack)
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
    this.debug&&this.debug.log("setActionOnMax")
    this.actionOnMax = callFunction
    return this
  }

  setActionOnEmpty (callFunction) {
    this.debug&&this.debug.log("setActionOnEmpty")
    this.ActionOnEmpty = callFunction
    return this
  }

  setAsync () {
    this.debug&&this.debug.log("setAsync")
     this.run = this.runAsync
     return this 
  }

  setAsyncShift () {
    this.debug&&this.debug.log("setAsyncShift")
    this.destructiveStack=true
    this.run = this.runAsyncShift
     return this
  }

  setConcurrent (concurrent) {
    this.debug&&this.debug.log("setConcurrent")
    this.concurrent = concurrent
    return this
  }

  setDebug (callFunction=this.debugFunction) {
    const _this=this
    this.debug = {log:(message)=>_this.debugFunction.log("processStack "+message)}
    this.debugFunction=callFunction
    this.debug&&this.debug.log("setDebug")
    return this 
  }
  setDebugOff () {
    this.debug&&this.debug.log("setDebugOff")
    this.debug=null
    return this 
  }

  setMaxWait (maxWait) {
    this.debug&&this.debug.log("setMaxWait")
    this.maxWait = maxWait
    return this 
  }

  setMaxWait (maxWait) {
    this.debug&&this.debug.log("setMaxWait")
    this.maxWait = maxWait
    return this 
  }

  setMaxDepth (maxDepth) {
    this.debug&&this.debug.log("setMaxDepth")
    if(this.destructiveStack===true){
      this.maxDepth = maxDepth
    } else throw Error('not destructive stack')
    if (!this.maxWait) this.setMaxWait(1000)
    return this
  }

  setNext () {
    this.debug&&this.debug.log("setNext")
    this.runAction = this.runNext
    return this
 }

  setNextPop () {
    this.debug&&this.debug.log("setNextPop")
    this.destructiveStack=true
    this.runAction = this.runNextPop
    return this
  }

  setNextShift () {
    this.debug&&this.debug.log("setNextShift")
    this.destructiveStack=true
    this.runAction = this.runNextShift
    return this
  }

  setRunnableTest (callFunction) {
    this.debug&&this.debug.log("setRunnableTest")
    this.isRunnable = callFunction
    return this
  }
  
  setSequentially () {
    this.debug&&this.debug.log("setSequentially")
    this.runAction = this.runSequentially
    return this 
  }

  setSequentiallyShift () {
    this.debug&&this.debug.log("setSequentiallyShift")
    this.destructiveStack=true
    this.runAction = this.runSequentiallyShift
    return this
  }

  setSequentiallyPop () {
    this.debug&&this.debug.log("setSequentiallyPop")
    this.destructiveStack=true
    this.runAction = this.runSequentiallyPop
    return this
  }

  settimeOut (time=1000) {
    this.timeOut
  }

  setWaitOff () {
    this.debug&&this.debug.log("setWaitOff")
    if (this.waitTimer) {
      clearTimeout(this.waitTimer)
      delete this.waitTimer
    }
    return this
  }

  setWaitOn () {
    this.debug&&this.debug.log("setWaitOn")
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
