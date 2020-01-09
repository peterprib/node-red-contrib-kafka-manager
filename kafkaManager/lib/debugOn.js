var debugCnt = 100
const ts = (new Date().toString()).split(' ')

exports.debugInit = (count, label) => {
  debugCnt = count
  if (count > 0) console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [info] ' + label + ' Copyright 2019 Jaroslav Peter Prib')
}

exports.debugOn = (m) => {
  if (debugCnt === 0) return
  console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [debug] ' + m.label + ' ' + (m instanceof Object ? JSON.stringify(m) : m))
  if (debugCnt === 1) console.log([parseInt(ts[2], 10), ts[1], ts[4]].join(' ') + ' - [debug] ' + m.label + ' debugging turn off')
  debugCnt--
}
