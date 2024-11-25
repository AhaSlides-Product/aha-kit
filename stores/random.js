const crypto = require('crypto')

const generateRandomString = (length) => {
  const array = new Uint8Array(length)
  crypto.getRandomValues(array)
  return Array.from(array, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

module.exports = {
  generateRandomString,
}
