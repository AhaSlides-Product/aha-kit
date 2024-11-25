const { LRUCache } = require('lru-cache')
const _ = require('lodash')

// At least one of 'max', 'ttl', or 'maxSize' is required, to prevent
// unsafe unbounded storage.
const defaultCreateOpts = {
  // In most cases, it's best to specify a max for performance, so all
  // the required memory allocation is done up-front.
  //
  // this one also the reason why we use LRU cache: we fixed size for cache
  max: 500,

  // place holdes
  // FIXME: when cached value is Promise, this one does not help
  maxSize: undefined,
  sizeCalculation: undefined,

  // make TTL on items work
  allowStale: false,
  updateAgeOnGet: false,
  updateAgeOnHas: false,
}

const _processOptions = (opts, defaultValues) => {
  const filteredOpts = _.pick(opts, Object.keys(defaultValues))
  return _.merge({}, defaultValues, filteredOpts)
}

const createStore = (opts) => {
  return new LRUCache(
    _processOptions(opts, defaultCreateOpts),
  )
}

const defaultSetOpts = {
  ttl: 1000 * 60 * 5,
}

const cacheAsideFunc = ({ store }) => ({ funcWoArgs, key, opts }) => {
  if (typeof funcWoArgs != 'function') {
    throw new TypeError('Expected a function')
  }

  const _setOpts = _processOptions(opts, defaultSetOpts)
  let cached = store.get(key)
  if (cached == undefined) {
    // even this one can be Promise, we don't await it here
    // this one is keypoint to protect thundering herd in nodejs
    cached = funcWoArgs()
    store.set(key, cached, _setOpts)
  }

  return cached
}

const set = (store) => (key, value, opts) => {
  const _setOpts = _processOptions(opts, defaultSetOpts)
  store.set(key, value, _setOpts)
}

const remove = (store) => (key) => {
  store.delete(key)
}

const purge = (store) => {
  store.clear()
}

module.exports = {
  createStore,
  set,
  remove,
  cacheAsideFunc,
  purge,
}
