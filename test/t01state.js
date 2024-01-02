/* global it, describe */
const assert = require('assert')
const State = require('../kafkaManager/state.js')
const { stat } = require('fs')
const showOn = (done,message = 'onUp', finalDone) => {
  console.log("showOn "+message)
  finalDone && finalDone()
  done()
}

describe('state', function () {
  it('initial', function (done) {
    const state = new State()
    assert.equal(state.isAvailable(), false)
    assert.equal(state.isNotAvailable(), true)
    done()
  })
  it('initial setUp', function (done) {
    const state = new State()
    state.setUp()
    assert.equal(state.isAvailable(), true)
    assert.equal(state.isNotAvailable(), false)
    assert.throws(() => state.setUp(), '')
    done()
  })
  it('initial setDown', function (done) {
    const state = new State()
    state.setUp().setDown()
    assert.equal(state.isAvailable(), false)
    assert.equal(state.isNotAvailable(), true)
    assert.throws(() => state.setDown(), '')
    done()
  })
  it('isAvailable functions down', function (done) {
    const state = new State()
    state.isAvailable(() => done('wrong call as down'), done)
  })
  it('isAvailable functions up', function (done) {
    const state = new State()
    state.up().isAvailable(done, () => done('wrong call as up'))
  })
  it('isNotAvailable functions Down', function (done) {
    const state = new State()
    state.isNotAvailable(done, () => done('wrong state down'))
  })
  it('isNotAvailable functions Up', function (done) {
    const state = new State()
    state.up().isNotAvailable(() => done('wrong state Up'), done)
  })
  it('up', function (done) {
    const state = new State()
    state.onUp(showOn.bind(this), 'onUp')
      .onDown((next) =>{
         done('onDown should not be called')
      })
      .up(()=>{
        console.log("up called")
        done()
      })
  })
  it('down', function (done) {
    const state = new State()
    state.onUp(showOn.bind(this), 'onUp OK')
      .onDown(showOn.bind(this), 'onDown OK')
      .up(() => { 
        state.down(done)
      })
  })
  it('onUp', function (done) {
    const state = new State()
    state.onUp(showOn.bind(this), 'onUp',done)
      .onDown((next) => {
        done('onDown should not be called')
        next();
      })
      .setUp()
  })
  it('onDown', function (done) {
    const state = new State()
      .onUp(showOn.bind(this),'onUp ok')
      .onDown(showOn.bind(this),'onDown ok', done)
      .setUp()
    state.setDown()
  })
  it('upAction', function (done) {
    const state = new State()
    let test
    state.setUpAction((done) => { test = true; done() })
      .onUp(showOn.bind(this), 'onUp ok')
      .onUp((next) => { if (test) done(); else done('call action no completed') ; next()})
      .onDown((next) =>{ done('onDown should not be called'); next() })
      .setUp()
  })
  it('upAction error', function (done) {
    const state = new State()
    let test
    state.setUpAction((next,error) => {
      console.log("call error")
      error(()=>{
        state.testDown()
        done()
      })
    })
      .onUp(showOn.bind(this), 'onUp ok')
      .onDown((next) =>{ done('onDown should not be called'); next() })
      .setUp()
  })
  it('downAction', function (done) {
    const state = new State()
    let test = ''
    state.setUpAction((next) => { test += 'setUpAction'; next() })
    state.setDownAction((next) => { test += ',setDownAction'; next() })
      .onUp((next) => { test += ',onUp'; next() })
      .onDown((next) => { 
        if (test === 'setUpAction,onUp,setDownAction') done()
        else done('call action no completed') 
        next()
      })
      .setUp()
    state.setDown()
  })
  it('whenUp', function (done) {
    const state = new State()
    let test = ''
    state.setUpAction((next) => { test += ',setUpAction'; next() })
    state.setDownAction((next) => { test += ',setDownAction'; next() })
      .onUp((next) => { test += ',onUp'; next() })
      .onDown((next) => { test += ',onDown'; next() })
      .whenUp((next) => { test += ',whenUp'; next() })
      .whenUp((next) => {
        if (test === ',setUpAction,onUp,whenUp') done()
        else done('test is ' + test) 
        next()
      })
      .setUp()
  })
  it('whenDown', function (done) {
    const state = new State()
    let test = ''
    const result=',setUpAction,onUp,whenUp,whenUp1,whenUp2,setDownAction,onDown'
    state.setUpAction((next) => { test += ',setUpAction'; next() })
    state
      .setDownAction((next) => { test += ',setDownAction'; next() })
      .onUp((next) => { test += ',onUp'; next() })
      .onDown((next) => { test += ',onDown'; next() })
      .whenUp((next) => { test += ',whenUp'; next() })
      .whenUp((next) => { test += ',whenUp1'; next() })
    console.log({ label: 'pre setup', state: state.getState() })
    state.setUp()
    console.log({ label: 'pre setup', state: state.getState() })
    state.whenUp((next) => { test += ',whenUp2'; next() })
    state.whenDown((next) => { 
      if (test === result) done(); 
      else done('result: ' + test + " expected: "+result)
      next()
    })
    console.log({ label: 'pre setdown', state: state.getState() })
    state.whenUp((next) => { state.setDown() ; next() })
  })
  it('setUp(done)', function (done) {
    const state = new State()
    let test = ''
    const result=',setUpAction,onUp'
    state.setUpAction((next) => { test += ',setUpAction'; next() })
    state.setDownAction((next) => { test += ',setDownAction'; next() })
      .onUp((next) => { test += ',onUp'; next() })
      .onDown((next) => { test += ',onDown'; next() })
      .whenUp((next) => { test += ',whenUp'; next() })
    state.setUp(() => {
      if (test === result) done()
      else done('result: ' + test + " expected: "+result)
    })
  })
  it('setDown(done)', function (done) {
    const state = new State()
    let test = ''
    const expecting = ',setUpAction,onUp,whenUp,whenUp1,setDownAction,onDown'
    state.setUpAction((next) => { test += ',setUpAction'; console.log(test); next() })
    state.setDownAction((next) => { test += ',setDownAction';  console.log(test); next() })
      .onUp((next) => { test += ',onUp' ;  console.log(test); next() })
      .onDown((next) => { test += ',onDown' ;  console.log(test); next() })
      .whenUp((next) => { test += ',whenUp' ;  console.log(test); next() })
    setTimeout(() =>{
        console.log('setTimeout1 paused .2 second')
        state.setUp((next) => {
          console.log("setTimeout1 up")
          state.whenUp((next) => { 
            test += ',whenUp1';  console.log(test); next() 
          })
          setTimeout(() =>{
            console.log("setTimeout2 setDown pause .2 secs")
            state.setDown((next) => {
              if (test === expecting) done()
              else done('expected: '+expecting+' result: ' + test)
              next()
            })
          }, 200)
          next() 
      })
      }, 200)  
  })
  it('setCheck', function (done) {
    const state = new State()
    let test = ''
    let checkCount = 0
    state.setUpAction((next) => { 
        test += ',setUpAction'; 
        next() 
    }).setDownAction((next) => { test += ',setDownAction'; next() })
      .onUp((next) => { test += ',onUp' ; next()})
      .onDown((next) => { test += ',onDown' ; next()})
      .whenUp((next) => { if (test === ',setCheck,setCheck,setUpAction,onUp') done(); else done('test is ' + test) ; next()})
      .setCheck((onUp, onDown) => {
        if (++checkCount > 2) return onUp()
        test += ',setCheck'
        return onDown()
      }, 100)
  })
  it('isTransitioning', function (done) {
    const state = new State()
    let up1 = false
    let up2 = false
    state.setUpAction((next) => { 
      setTimeout(() => { 
        console.log('up'); 
        next()
      }, 1000) })
    state.setUp((next) => {
      if (up2 === true) done('second up done')
      up1 = true
      console.log('up1 done') 
      next()
    })
    state.whenUp((next) => {
      console.log('whenup')
      if (up1 === true && up2 === true) done()
      else done('whenUp actioned too early up1:' + up1 + ' up2:' + up2 + ' stack:' + state.stack.setUpDone.length)
      next()
    })
    setTimeout(() =>{
      try{
        state.setUp((next) => {
          if (up1 === false) return done('first up not done')
          up2 = true
          console.log('up2 done')
          next()
        })
      } catch(ex) {
        console.log("setUp2 failed "+ex.message)
      }
    }, 200)
  })
  it('beforeUp', function (done) {
    const state = new State()
    let sequence = ''
    const expectedSequence = ',beforeUp1,beforeUp2,beforeUp3,upAction,onUp1'
    state
      .onUp((next) => { sequence += ',onUp1' ; next()})
      .beforeUp((next) => { sequence += ',beforeUp1'; next() })
      .setUpAction((next) => { setTimeout(() => { console.log('up'); sequence += ',upAction'; next() }, 1000) })
      .beforeUp((next) => { sequence += ',beforeUp2'; next() })
      .beforeUp((next) => { sequence += ',beforeUp3'; next() })
      .setUp(() => console.log('setUp done'))
      .whenUp((next) => {
        console.log('whenup')
        if (sequence === expectedSequence) done()
        else done('expected:' + expectedSequence + ' actual:' + sequence)
        next()
      })
  })
  it('beforeDown', function (done) {
    const state = new State()
    let sequence = ''
    const expectedSequence = ',setUp,whenUp,beforeDown1,beforeDown2,beforeDown3,downAction,downActionTimeout,onDown1,setDown,whenDown'
    state
      .onDown((next) => { sequence += ',onDown1' ; console.log(sequence); next()})
      .beforeDown((next) => { sequence += ',beforeDown1'; console.log(sequence); next() })
      .setDownAction((next) => {
        sequence += ',downAction'; console.log(sequence)
        setTimeout(() => {
          sequence += ',downActionTimeout'; console.log(sequence)
          next() 
        }, 500) 
      })
      .beforeDown((next) => { sequence += ',beforeDown2'; console.log(sequence); next() })
      .beforeDown((next) => { sequence += ',beforeDown3'; console.log(sequence); next() })
      .setUp(() => { sequence += ',setUp'; console.log(sequence);  next() })
      .whenUp((next) => {
        sequence += ',whenUp'; console.log(sequence);
        state.whenDown((next) => {
          sequence += ',whenDown'; console.log(sequence);
          if (sequence === expectedSequence) done()
          else done('expected:' + expectedSequence + ' actual:' + sequence)
          next()
        })
        .setDown(() => { sequence += ',setDown'; console.log(sequence);  next() })
        next()
      })
      console.log("***beforeDown wait on done")
  }).timeout(4000)
  it('setUpFail upFailedAndClearQ', function (done) {
    const state = new State()
    .setLogLevel1()
    const testMessage = 'test message'
    let test = ''
    state.setUpAction((next) => { 
      test += ',setUpAction'; console.log(test)
      state.upFailedAndClearQ(testMessage,next)
//      next()
    })
    state.setDownAction((next) => { test += ',setDownAction'; console.log(test); next() })
      .onDown((next) => { test += ',onDown'; console.log(test) ; next()})
    state.whenUp((next) => { done('fail whenup1') ; next()})
    state.whenUp({
      callFunction: (next) => {
        done('fail whenup2')
        next()
      },
      onDisgard: (message,next) => {
        test += ',whenUp2 onDisgard'; console.log(test)
        if (message === testMessage) done()
        else done('wrong message: ' + message)
        next()
      }
    })
    state.whenUp((next) => { done('fail whenup3'); next() })
    state.setUp((done)=>{
      test += ',setUp'; console.log(test); next()
    })
    console.log("***setUpFail upFailedAndClearQ wait on done")
  })
  it('on Idle 200', function (done) {
    let sequence = ''
    const expectedSequence = ',upAction,onUp,whenup1,beforeDown1,downAction,onDown,whenDown,onIdle,setTimeout,upAction,onUp,whenup2,beforeDown1,downAction,onDown,onIdle,whenDownTimeout'
    const state = new State()
//    .setLogLevel1()
    state.setUpAction((next) => {
      sequence += ',upAction'; console.log(sequence); 
      next() 
    }).setUpOnUpQDepth(0).setIdleTime(200).onIdle((next) => {
      sequence += ',onIdle'; console.log(sequence); 
      next()
    }).onDown((next) => {
      sequence += ',onDown'; console.log(sequence); 
      next()
    }).onUp((next) => {
      sequence += ',onUp'; console.log(sequence);
      next();
    }).beforeDown((next) => {
      sequence += ',beforeDown1'; console.log(sequence)
      next()
    }).setDownAction((next) => { 
        sequence += ',downAction'; console.log(sequence);
        next()
    }).whenUp((next) => {
      sequence += ',whenup1';console.log(sequence)
      state.whenDown((nextWhenDown) => {
        sequence += ',whenDown'; console.log(sequence);
        console.log('*** fire when up in 1000')
        setTimeout(() => {
          console.log('*** 1000 has past, whenDownTimeout')
          sequence += ',whenDownTimeout'; console.log(sequence)
          if (sequence === expectedSequence) done()
          else done('\nexpected:' + expectedSequence + '\n  actual:' + sequence)
          }, 1000)
          nextWhenDown() 
      })
      next()
    })
    console.log('*** fire when up in 800')
    setTimeout(() => {
      console.log('*** 800 has past, whenup2')
      sequence += ',setTimeout'; console.log(sequence);
      state.whenUp((next) => {
        sequence += ',whenup2'; console.log(sequence);
        next()
      })
    }, 800)
    console.log("*** idle ended now waiting on final done")
  }).timeout(4000)
}).timeout(10000)
