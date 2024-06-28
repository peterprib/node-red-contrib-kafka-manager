/* global it, describe */
const assert = require('assert')
const ProcessStack = require('../kafkaManager/processStack.js')

describe('ProcessStack', function () {
  it('no entry runNext', function (done) {
    const stack = new ProcessStack()
    stack.runNext(done)
  })
  it('no entry Sequentially', function (done) {
    const stack = new ProcessStack()
    stack.runSequentially()
    done()
  })
  it('no entry runNext+ run callback', function (done) {
    const stack = new ProcessStack()
    stack.runNext(() => {
      console.log('done')
      done()
    })
  })
  it('no entry Sequentially + run callback', function (done) {
    const stack = new ProcessStack()
    stack.runSequentially(() => {
      console.log('done')
      done()
    })
  })
  it('one entry runNext', function (done) {
    let test = 0; const expected = 1
    const stack = new ProcessStack()
    stack.add((next) => {
      console.log(++test)
      next()
    }).runNext(() => {
      if (test === expected) done()
      else done('failed expected: ' + expected + ' result: ' + test)
    })
  })
  it('one entry Sequentially', function (done) {
    const stack = new ProcessStack().add(done)
    stack.runSequentially()
  })
  it('three entries Sequentially', function (done) {
    let test = ''; const expected = ',1,2,3'
    const stack = new ProcessStack()
      .add(() => { test += ',1' })
      .add(() => { test += ',2' })
      .add(() => { test += ',3' })
    stack.runSequentially(() => {
      if (test === expected) done()
      else done('failed expected: ' + expected + ' result: ' + test)
    })
  })
  it('three entry runNext', function (done) {
    let test = 0; const expected = 3
    const process = (next) => {
      console.log(++test)
      next()
    }
    const stack = new ProcessStack()
    stack.add(process).add(process).add(process).runNext(() => {
      if (test === expected) done()
      else done('failed expected: ' + expected + ' result: ' + test)
    })
  })
  describe('ProcessStack Shift', function () {
    it('three entries', function (done) {
      let test = ''; const expected = ',1,2,3'
      const stack = new ProcessStack()
        .add((next) => { test += ',1'; next() })
        .add((next) => { test += ',2'; next() })
        .add((next) => { test += ',3'; next() })
      stack.runNextShift(() => {
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
    it('three entries', function (done) {
      let test = ''; const expected = ',1,2,3'
      const stack = new ProcessStack()
        .add((next) => { test += ',1'; next() })
        .add((next) => { test += ',2'; next() })
        .add((next) => { test += ',3'; next() })
      stack.runNextShift(() => {
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
  })
  describe('ProcessStack Pop', function () {
    it('three entries', function (done) {
      let test = ''; const expected = ',3,2,1'
      const stack = new ProcessStack()
        .add((next) => { test += ',1'; next() })
        .add((next) => { test += ',2'; next() })
        .add((next) => { test += ',3'; next() })
      stack.runNextPop(() => {
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
  })
  describe('ProcessStack runNextAsync', function () {
    it('zero entries', function (done) {
      const test = 0; const expected = 0
      const stack = new ProcessStack()
      .setDebug()
      stack.runAsync(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
    it('one entries', function (done) {
      let test = 0; const expected = 1
      const stack = new ProcessStack()
      .setDebug()
        .add(() => { console.log(1); test += 1; })
      stack.runAsync(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
    it('three entries', function (done) {
      let test = 0; const expected = 1 + 2 + 3
      const stack = new ProcessStack()
        .add(() => { console.log(1); test += 1;})
        .add(() => { console.log(2); test += 2;})
        .add(() => { console.log(3); test += 3;})
      stack.runAsync(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
    it('six entries concurrent 2', function (done) {
      let test = 0; const expected = 1 + 2 + 3 + 4 + 5 + 6
      const stack = new ProcessStack().setConcurrent(2)
        .add(() => { console.log(1); test += 1;  })
        .add(() => { console.log(2); test += 2;  })
        .add(() => { console.log(3); test += 3;  })
        .add(() => { console.log(4); test += 4;  })
        .add(() => { console.log(5); test += 5;  })
        .add(() => { console.log(6); test += 6;  })
      stack.runAsync(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
  })
  describe('ProcessStack runNextAsyncShift', function () {
    it('zero entries', function (done) {
      const test = 0; const expected = 0
      const stack = new ProcessStack()
      .setDebug()
      stack.runAsyncShift(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
    it('one entries', function (done) {
      let test = 0; const expected = 1
      const stack = new ProcessStack()
        .setDebug()
        .add(() => { console.log(1); test += 1})
      stack.runAsyncShift(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
    it('three entries', function (done) {
      let test = 0; const expected = 1 + 2 + 3
      const stack = new ProcessStack()
        .add(() => { console.log(1); test += 1;  })
        .add(() => { console.log(2); test += 2;  })
        .add(() => { console.log(3); test += 3;  })
      stack.runAsyncShift(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
    it('six entries concurrent 2', function (done) {
      let test = 0; const expected = 1 + 2 + 3 + 4 + 5 + 6
      const stack = new ProcessStack().setConcurrent(2)
        .add(() => { console.log(1); test += 1;  })
        .add(() => { console.log(2); test += 2;  })
        .add(() => { console.log(3); test += 3;  })
        .add(() => { console.log(4); test += 4;  })
        .add(() => { console.log(5); test += 5;  })
        .add(() => { console.log(6); test += 6;  })
      stack.runAsyncShift(() => {
        console.log('end')
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
    })
  })
})

describe('runSequentiallyShift', function () {
  it('zero entries', function (done) {
    const test = 0; const expected = 0
    const stack = new ProcessStack()
    stack.setSequentiallyShift().run(() => {
      console.log('end')
      if (test === expected) done()
      else done('failed expected: ' + expected + ' result: ' + test)
    })
  })

  it('three entries', function (done) {
    let test = 0; const expected = 1 + 2 + 3
    const stack = new ProcessStack()
      .add((next) => { console.log(1); test += 1 })
      .add((next) => { console.log(2); test += 2 })
      .add((next) => { console.log(3); test += 3 })
    stack.setSequentiallyShift().run(() => {
      console.log('end')
      if (test === expected) done()
      else done('failed expected: ' + expected + ' result: ' + test)
    })
  })
})

describe('runSequentiallyPop', function () {
  it('zero entries', function (done) {
    const test = 0; const expected = 0
    const stack = new ProcessStack()
    stack.setSequentiallyPop().run(() => {
      console.log('end')
      if (test === expected) done()
      else done('failed expected: ' + expected + ' result: ' + test)
    })
  })

  it('three entries', function (done) {
    let test = 0; const expected = 1 + 2 + 3
    const stack = new ProcessStack()
      .add((next) => { console.log(1); test += 1 })
      .add((next) => { console.log(2); test += 2 })
      .add((next) => { console.log(3); test += 3 })
    stack.setSequentiallyPop().run(() => {
      console.log('end')
      if (test === expected) done()
      else done('failed expected: ' + expected + ' result: ' + test)
    })
  })
})

describe('setActionOnMax', function () {
  it('three entries', function (done) {
    let test = 0; const expected = 1 + 2 + 3
    const stack = new ProcessStack()
      .setSequentiallyPop()
      .setMaxDepth(2)
    stack
      .add(() => { console.log(1); test += 1 })
      .add(() => { console.log(2); test += 2 })
      .add(() => { console.log(3); test += 3 })
      .add(() => { console.log(4); test += 4 })
      .add(() => {
        console.log('last add');
        if (test === expected) done()
        else done('failed expected: ' + expected + ' result: ' + test)
      })
  })
})

describe('setActionOnEmpty', function () {
})
