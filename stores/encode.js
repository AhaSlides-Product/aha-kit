const needsMarshalling = (value) => {
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

  // Default: keep as string
  return value
}

const jsonMarsh = {
  marshall: (obj) => {
    if (!needsMarshalling(obj)) {
      return obj
    }
    return JSON.stringify(obj)
  },
  unmarshall: (biData) => {
    if (biData == null) {
      return null
    }
    return safeParseJSON(biData)
  },
}

module.exports = {
  needsMarshalling,
  safeParseJSON,
  jsonMarsh,
}
