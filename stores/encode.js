const msgpack = require('msgpack-lite')

const needEncode = (value) => {
  return typeof value !== 'string' && typeof value !== 'number'
}

const safeParseJSON = (value) => {
  if (!isNaN(value)) {
    return Number(value)
  }

  if (value === 'true' || value === 'false') {
    return value === 'true'
  }

  if (value?.startsWith('{') || value?.startsWith('[')) {
    return JSON.parse(value)
  }

  return value
}

const jsonEncDec = {
  encode: (obj) => {
    if (!needEncode(obj)) {
      return obj
    }
    return JSON.stringify(obj)
  },
  decode: (biData) => {
    return safeParseJSON(biData)
  },
}

const msgpackEncDec = {
  encode: (obj) => {
    if (!needEncode(obj)) {
      return obj
    }
    return msgpack.encode(obj)
  },
  decode: (biData) => {
    if (biData == null) {
      return null
    }
    return msgpack.decode(biData)
  },
}

module.exports = {
  needEncode,
  safeParseJSON,
  jsonEncDec,
  msgpackEncDec,
}
