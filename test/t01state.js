/* eslint-disable no-undef */
const assert = require('assert')
const State = require('../kafkaManager/state.js')
const showOn = (message = 'onUp', done) => {
  console.log(message)
  done && done()
}

describe('state', function () {
  it('initial', function (done) {
    const state = new State()
    assert.equal(state.isAvailable(), false)
    assert.equal(state.isNotAvailable(), true)
    state.setUp()
    assert.equal(state.isAvailable(), true)
    assert.equal(state.isNotAvailable(), false)
    assert.throws(() => state.setUp(), '')
    state.setDown()
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
      .onDown(() => done('onDown should not be called'))
      .up(done)
  })
  it('down', function (done) {
    const state = new State()
    state.onUp(showOn.bind(this), 'onUp OK')
      .onDown(showOn.bind(this), 'onDown OK')
      .up(() => state.down(done))
  })
  it('onUp', function (done) {
    const state = new State()
    state.onUp(showOn.bind(this), 'onUp', done)
      .onDown(() => done('onDown should not be called'))
      .setUp()
  })
  it('onDown', function (done) {
    const state = new State()
      .onUp(showOn.bind(this), 'onUp ok')
      .onDown(showOn.bind(this), 'onDown ok', done)
      .setUp()
    state.setDown()
  })
  it('upAction', function (done) {
    const state = new State()
    let test
    state.setUpAction((done) => { test = true; done() })
      .onUp(showOn.bind(this), 'onUp ok')
      .onUp(() => { if (test) done(); else done('call action no completed') })
      .onDown(() => done('onDown should not be called'))
      .setUp()
  })
  it('downAction', function (done) {
    const state = new State()
    let test = ''
    state.setUpAction((done) => { test += 'setUpAction'; done() })
    state.setDownAction((done) => { test += ',setDownAction'; done() })
      .onUp(() => { test += ',onUp' })
      .onDown(() => { if (test === 'setUpAction,onUp,setDownAction') done(); else done('call action no completed') })
      .setUp()
    state.setDown()
  })
  it('whenUp', function (done) {
    const state = new State()
    let test = ''
    state.setUpAction((done) => { test += 'setUpAction'; done() })
    state.setDownAction((done) => { test += ',setDownAction'; done() })
      .onUp(() => { test += ',onUp' })
      .onDown(() => { test += ',onDown' })
      .whenUp(() => { test += ',whenUp' })
      .whenUp(() => { if (test === 'setUpAction,onUp,whenUp') done(); else done('test is ' + test) })
      .setUp()
  })
  it('whenDown', function (done) {
    const state = new State()
    let test = ''
    state.setUpAction((done) => { test += ',setUpAction'; done() })
    state.setDownAction((done) => { test += ',setDownAction'; done() })
      .onUp(() => { test += ',onUp' })
      .onDown(() => { test += ',onDown' })
      .whenUp(() => { test += ',whenUp' })
      .whenUp(() => { test += ',whenUp1' })
    console.log({ label: 'pre setup', state: state.getState() })
    state.setUp()
    console.log({ label: 'pre setup', state: state.getState() })
    state.whenUp(() => { test += ',whenUp2' })
    state.whenDown(() => { if (test === ',setUpAction,onUp,whenUp,whenUp1,whenUp2,setDownAction,onDown') done(); else done('test is ' + test) })
    console.log({ label: 'pre setdown', state: state.getState() })
    state.whenUp(() => { state.setDown() })
  })
  it('setUp(done)', function (done) {
    const state = new State()
    let test = ''
    state.setUpAction((done) => { test += ',setUpAction'; done() })
    state.setDownAction((done) => { test += ',setDownAction'; done() })
      .onUp(() => { test += ',onUp' })
      .onDown(() => { test += ',onDown' })
      .whenUp(() => { test += ',whenUp' })
    state.setUp(() => { if (test === ',setUpAction,onUp') done(); else done('test is ' + test) })
  })
  it('setDown(done)', function (done) {
    const state = new State()
    let test = ''
    state.setUpAction((done) => { test += ',setUpAction'; done() })
    state.setDownAction((done) => { test += ',setDownAction'; done() })
      .onUp(() => { test += ',onUp' })
      .onDown(() => { test += ',onDown' })
    state.setUp(() => {
      state.whenUp(() => { test += ',whenUp' })
      state.setDown(() => { if (test === ',setUpAction,onUp,whenUp,setDownAction,onDown') done(); else done('test is ' + test) })
    })
  })
  it('setCheck', function (done) {
    const state = new State()
    let test = ''
    let checkCount = 0
    state.setUpAction((done) => { test += ',setUpAction'; done() })
      .setDownAction((done) => { test += ',setDownAction'; done() })
      .onUp(() => { test += ',onUp' })
      .onDown(() => { test += ',onDown' })
      .whenUp(() => { if (test === ',setCheck,setCheck,setUpAction,onUp') done(); else done('test is ' + test) })
      .setCheck((onUp, onDown) => {
        if (++checkCount > 2) return onUp()
        test += ',setCheck'
        return onDown()
      }, 100)
  })
  it('isTranstioning', function (done) {
    const state = new State()
    let up1 = false
    let up2 = false
    state.setUpAction((doneUp) => { setTimeout(() => { console.log('up'); doneUp() }, 1000) })
    state.setUp(() => { if (up2 === true) done('second up done'); up1 = true; console.log('up1 done') })
    state.whenUp(() => {
      console.log('whenup')
      if (up1 === true && up2 === true) done()
      else done('whenUp actioned too early up1:' + up1 + ' up2:' + up2 + ' stack:' + state.stack.setUpDone.length)
    })
    state.setUp(() => { if (up1 === false) return done('first up not done'); up2 = true; console.log('up2 done') })
  })
  it('beforeUp', function (done) {
    const state = new State()
    let sequence = ''
    const expectedSequence = ',beforeUp1,beforeUp2,beforeUp3,upAction,onUp1'
    state
      .onUp(() => { sequence += ',onUp1' })
      .beforeUp((done) => { sequence += ',beforeUp1'; done() })
      .setUpAction((doneUp) => { setTimeout(() => { console.log('up'); sequence += ',upAction'; doneUp() }, 1000) })
      .beforeUp((done) => { sequence += ',beforeUp2'; done() })
      .beforeUp((done) => { sequence += ',beforeUp3'; done() })
      .setUp(() => console.log('setUp done'))
      .whenUp(() => {
        console.log('whenup')
        if (sequence === expectedSequence) done()
        else done('expected:' + expectedSequence + ' actual:' + sequence)
      })
  })
  it('beforeDown', function (done) {
    const state = new State()
    let sequence = ''
    const expectedSequence = ',beforeDown1,beforeDown2,beforeDown3,downAction,onDown1'
    state
      .onDown(() => { sequence += ',onDown1' })
      .beforeDown((done) => { sequence += ',beforeDown1'; done() })
      .setDownAction((doneDown) => { setTimeout(() => { console.log('down'); sequence += ',downAction'; doneDown() }, 1000) })
      .beforeDown((done) => { sequence += ',beforeDown2'; done() })
      .beforeDown((done) => { sequence += ',beforeDown3'; done() })
      .setUp(() => console.log('setUp done'))
      .whenUp(() => {
        state.whenDown(() => {
          console.log('whenDown')
          if (sequence === expectedSequence) done()
          else done('expected:' + expectedSequence + ' actual:' + sequence)
        }).setDown(() => console.log('setDown done'))
      })
  })
  it('on Idle', function (done) {
    let sequence = ''
    const expectedSequence = ',onUp,whenup1,setTimeout,whenup2,beforeDown1,onIdle,downAction'
    const state = new State()
    state.setUpAction((upDone) =>{console.log('up action'); upDone() })
      .setUpOnUpQDepth(0).setIdleTime(1000)
      .onIdle((idleDone) => {
        console.log('onIdle: '+sequence)
        sequence += ',onIdle'
        idleDone()
      }).onDown(() => {
        console.log('onDown')
        if (sequence === expectedSequence) done()
        else done('expected:' + expectedSequence + ' actual:' + sequence)
      })
      .onUp(() => {
        console.log('onUp')
        sequence += ',onUp'
      })
      .beforeDown((beforeDone) => { sequence += ',beforeDown1'; beforeDone() })
      .setDownAction((doneDown) => { setTimeout(() => { console.log('down'); sequence += ',downAction'; doneDown() }, 1000) })
      .whenUp(() => {
        console.log('when1: ' + sequence)
        sequence += ',whenup1'
      })
    console.log('fire when up in 800')
    setTimeout(() => {
      console.log('setTimout extend use')
      sequence += ',setTimeout'
      state.whenUp(() => {
        console.log('setTimout extend use whenup')
        sequence += ',whenup2'
      })
    }, 800)
    console.log('end script')
  }).timeout(4000);
})
