const isPromise = (value) => {
  return value &&
    typeof value === 'object' &&
    typeof value.then === 'function' &&
    Promise.resolve(value) === value
}

module.exports = {
  isPromise: isPromise,
}
