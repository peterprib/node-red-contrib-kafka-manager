const equals = (v1, v2, keys) => keys.every(k => v1[k] == v2[k], true)
const notEquals = (v1, v2, keys) => !equals(v1, v2, keys)
const includes = (a1, o, keys) => a1.find(c => equals(c, o, keys), true)
const excludes = (v1, o, keys) => !includes(v1, o, keys)

function compareArrays (left = [], right = [], keys) {
  if (keys) {
    return {
      left: right.filter(c => excludes(left, c, keys)),
      right: left.filter(c => excludes(right, c, keys))
    }
  }
  return {
    left: right.filter(c => !left.includes(c)),
    right: left.filter(c => !right.includes(c))
  }
}
module.exports = compareArrays
