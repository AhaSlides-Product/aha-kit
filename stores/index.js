const {
  createClient: createRedisClient,
  cacheAsideFunc: redisCacheAsideFunc,
} = require('./redis')
const {
  createStore: createInAppStore,
  remove: removeInApp,
  cacheAsideFunc: inAppCacheAsideFunc,
} = require('./inAppLru')

const isPromise = (value) => {
  return value &&
    typeof value === 'object' &&
    typeof value.then === 'function' &&
    Promise.resolve(value) === value
}

const create = ({ inAppStore, redisReplicaClient, redisMasterClient }) => {
  const inAppCacheReadThrough = ({ funcWoArgs, key, ttlMs }) => {
    return inAppCacheAsideFunc({ store: inAppStore })({
      funcWoArgs,
      key,
      opts: { ttl: ttlMs },
    })
  }

  const redisCacheReadThrough = ({
    func, key,
    ttlMs, inAppTtlMs,
  }) => {
    return (...args) => {
      const funcWoArgs = () => func(...args)

      const redisWrappedWoArgsFunc = () => {
        const fetcher = redisCacheAsideFunc({ funcWoArgs, key, ttlMs })
        return fetcher({
          replicaClient: redisReplicaClient,
          masterClient: redisMasterClient,
        })
      }

      return inAppCacheReadThrough({
        funcWoArgs: redisWrappedWoArgsFunc,
        key,
        ttlMs: inAppTtlMs,
      })
    }
  }

  const redisCacheOnlyReadThrough = async ({ func, key, ttlMs }) => {
    let cached = redisCacheReadThrough({ func, ttlMs, inAppTtlMs: 0 })
    if (isPromise(cached)) {
      cached = await cached
    }
    removeInApp(inAppStore)(key)
    return cached
  }

  return {
    inAppCacheReadThrough,
    redisCacheReadThrough,
    redisCacheOnlyReadThrough,
  }
}

module.exports = {
  createInAppStore,
  createRedisClient,
  create,
}
