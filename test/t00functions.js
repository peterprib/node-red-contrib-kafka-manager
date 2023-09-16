/* eslint-disable no-undef */
const assert = require('assert')
const compareArrays = require('../kafkaManager/compareArrays.js')
const filterArray = require('../kafkaManager/filterArray.js')

const left = ['a', 'b', 'c', 'd']
const right = ['a', 'b', 'c1', 'd2']
const noDiff = { left: [], right: [] }
// const diff = { left: ['c', 'd'], right: ['c1', 'd2'] }

const leftKeys = [{ key1: 'a', key2: 0, data: 't1a0' }, { key1: 'b', key2: 0, data: 'tb0' }, { key1: 'c', key2: 0, data: 'tc0' }, { key1: 'd', key2: 0, data: 'td0' }]
const rightKeys = [{ key1: 'a', key2: 0, data: 't1a0' }, { key1: 'b', key2: 0, data: 'tb0' }, { key1: 'c', key2: 1, data: 'tc1' }, { key1: 'd', key2: 2, data: 'td2' }]
const keys = ['key1', 'key2']
describe('compareArrays', function () {
  it('nulls', function (done) {
    assert.deepEqual(compareArrays(), noDiff)
    done()
  })
  it('same', function (done) {
    assert.deepEqual(compareArrays(left, left), noDiff)
    done()
  })
  it('diff left', function (done) {
    assert.deepEqual(compareArrays(left), { left: [], right: left })
    done()
  })
  it('diff right', function (done) {
    assert.deepEqual(compareArrays([], right), { left: right, right: [] })
    done()
  })
  it('diff', function (done) {
    assert.deepEqual(compareArrays(left, right), { left: ['c1', 'd2'], right: ['c', 'd'] })
    done()
  })
})
describe('compare Array of objects', function () {
  it('nulls', function (done) {
    assert.deepEqual(compareArrays([], [], keys), noDiff)
    done()
  })
  it('same', function (done) {
    assert.deepEqual(compareArrays(leftKeys, leftKeys, keys), noDiff)
    done()
  })
  it('diff left', function (done) {
    assert.deepEqual(compareArrays(leftKeys, [], keys), { left: [], right: leftKeys })
    done()
  })
  it('diff right', function (done) {
    assert.deepEqual(compareArrays([], rightKeys, keys), { left: rightKeys, right: [] })
    done()
  })
  it('diff ', function (done) {
    assert.deepEqual(compareArrays(leftKeys, rightKeys, keys),
      {
        right: [{ key1: 'c', key2: 0, data: 'tc0' }, { key1: 'd', key2: 0, data: 'td0' }],
        left: [{ key1: 'c', key2: 1, data: 'tc1' }, { key1: 'd', key2: 2, data: 'td2' }]
      })
    done()
  })
})

describe('filterArrays', function () {
  it('all', function (done) {
    assert.deepEqual(filterArray(left, '.*'), left)
    done()
  })
  it('filter 1 filter', function (done) {
    assert.deepEqual(filterArray(left, 'a.*'), ['a'])
    assert.deepEqual(filterArray(['a', 'b', 'c1', 'c2', 'd2'], 'c.*'), ['c1', 'c2'])
    done()
  })
  it('filter 2 filter', function (done) {
    assert.deepEqual(filterArray(left, ['b', 'c']), ['b', 'c'])
    done()
  })
})
