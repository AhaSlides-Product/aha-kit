const {
  createClient: createRedisClient,
  cacheAsideFunc,
} = require('./redis')
const { isPromise } = require('./promise')
const {
  createStore: createInAppStore,
  remove: removeInApp,
  cacheAsideFunc: inAppCacheAsideFunc,
} = require('./inAppLru')
const {
  msgpackEncDec,
} = require('./encode')
const {
  snappyCompr,
} = require('./compress')

const msgpackSnappyMarsh = {
  marshall: (obj) => {
    if (obj == null) {
      throw new Error('marshall null data')
    }
    return snappyCompr.compressAsync({
      biData: msgpackEncDec.encode(obj),
    })
  },
  unmarshall: async (biData) => {
    if (biData == null) {
      return null
    }
    const uncompressed = await snappyCompr.uncompressAsync({ biData })
    return msgpackEncDec.decode(uncompressed)
  }
}

const redisCacheAsideFunc = cacheAsideFunc({
  marshallFunc: msgpackSnappyMarsh.marshall,
  unmarshallFunc: msgpackSnappyMarsh.unmarshall,
})

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

  const redisCacheOnlyReadThrough = async ({ funcWoArgs, key, ttlMs }) => {
    let cached = redisCacheReadThrough({
      func: funcWoArgs,
      key,
      ttlMs,
      inAppTtlMs: 0,
    })()
    if (isPromise(cached)) {
      cached = await cached
    }
    removeInApp(inAppStore)(key)
    return cached
  }

  const redisCacheWrite = async ({ data, key, ttlMs }) => {
    const dataToSet = msgpackSnappyMarsh.marshall(data)
    return redisMasterClient.set(key, dataToSet, { PX: ttlMs })
  }

  return {
    inAppCacheReadThrough,
    redisCacheReadThrough,
    redisCacheOnlyReadThrough,
    redisCacheWrite,
  }
}

module.exports = {
  msgpackSnappyMarsh,

  createInAppStore,
  createRedisClient,
  create,
}
