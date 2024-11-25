const crypto = require('crypto')

function recursiveStringify(value, depth = 0, maxDepth = 3) {
  if (depth > maxDepth) {
    return `maxDepth:${maxDepth}`
  }

  if (value === null) {
    return 'null'
  }

  if (typeof value === 'function') {
    return value.name
  }

  if (typeof value === 'object') {
    const sortedKeys = Object.keys(value).sort().slice(0, maxDepth)
    const sortedObj = {}

    for (let i = 0; i < sortedKeys.length; i++) {
      const key = sortedKeys[i]
      sortedObj[key] = recursiveStringify(value[key], depth + 1, maxDepth)
    }

    return JSON.stringify(sortedObj)
  }

  return String(value)
}

function hashArray(arr) {
  const normalizedElements = arr.map(recursiveStringify)
  const combinedString = normalizedElements.join('|')

  const hash = crypto.createHash('sha256')
  hash.update(combinedString)
  return hash.digest('hex')
}

module.exports = {
  hashArray,
}
