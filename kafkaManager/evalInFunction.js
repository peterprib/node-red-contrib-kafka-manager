function evalFunction (id, mapping) {
  try {
    return eval(mapping)
  } catch (ex) {
    throw Error(id + ' ' + ex.message)
  }
}
function evalInFunction (node, propertyName) {
  try {
	    const nodeContext = node.context()
    const flow = nodeContext.flow
    const global = nodeContext.global
    const property = node[propertyName]
    if (property == null) throw Error('no value for ' + propertyName)
    const propertyType = propertyName + '-type'
    switch (node[propertyType]) {
      case 'str':
        return evalFunction(propertyName, '()=>' + JSON.stringify(node.property))
      case 'num':
      case 'json':
        return evalFunction(propertyName, '()=>' + property)
      case 'node':
        return evalFunction(propertyName, '()=>nodeContext.get(' + property + ')')
      case 'flow':
        if (flow) throw Error("context store may be memoryonly so flow doesn't work")
        return evalFunction(propertyName, '()=>flow.get(' + property + ')')
      case 'global':
        return evalFunction(propertyName, '()=>global.get(' + property + ')')
      case 'env':
        return evalFunction(propertyName, '()=>process.env[' + property + ']')
      case 'msg':
        return evalFunction(propertyName, '(msg)=>msg.' + property+"||undefined")
      default:
        throw Error('unknown type ' + node[propertyType])
    }
  } catch (ex) {
    throw Error(propertyName + ' ' + ex.message)
  }
}
module.exports = evalInFunction
