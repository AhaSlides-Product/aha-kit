const redis = require('redis-support-transaction')
const { generateRandomString } = require('./random')
const { tryWithBackoffRetry } = require('./retry')

// lock impl requirements
// - eventually can aquire
// - no deadlock
// - only lock owner can unlock
// ref: https://redis.io/docs/latest/develop/use/patterns/distributed-locks/
//
// consider use redlock when run in cluster mode
// ref: https://github.com/mike-marcacci/node-redlock
class AquireLockError extends Error {
  constructor(message) {
    super(message)
  }
}

const simpleLock = {
  lockName: (name) => `simpleLock.${name}`,
  aquire: ({ retryTimeoutMs }) => async ({ masterClient, name, ttlMs }) => {
    const lockName = simpleLock.lockName(name)
    // secret used here to ensure ownership
    const secret = generateRandomString(8) + Date.now()

    const trySet = async () => {
      const result = await masterClient.set(lockName, secret, { PX: ttlMs, NX: true })
      if (result === null) {
        throw new AquireLockError('aquire failed')
      }
    }

    await tryWithBackoffRetry({
      funcWoArgs: trySet,
      maxTimeMs: retryTimeoutMs,
      allowedErrorType: AquireLockError,
    })

    return {
      release: () => simpleLock.release({ masterClient, name: lockName, secret }),
      extend: ({ ttlMs: _ttlMs }) => simpleLock.extend({ masterClient, name: lockName, secret, ttlMs: _ttlMs }),
    }
  },
  release: async ({ masterClient, name, secret }) => {
    const script = `
      if redis.call("get",KEYS[1]) == ARGV[1] then
          return redis.call("del",KEYS[1])
      else
          return 0
      end
    `
    const res = await masterClient.eval(script, { keys: [name], arguments: [secret] })
    if (res === 0) {
      throw new Error('release failed')
    }
  },
  extend: async ({ masterClient, name, secret, ttlMs }) => {
    const script = `
      if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("set", KEYS[1], ARGV[1], "PX", ARGV[2])
      else
          return 0
      end
      `
    const res = await masterClient.eval(script, { keys: [name], arguments: [secret, String(ttlMs)] })
    if (res === 0) {
      throw new Error('extend failed')
    }
  },
}

const wrapWithPessimisticSimpleLock = async ({
  masterClient,
  funcWoArgs,
  key,
  lockTimeMs=5000,
  aquireLockTimeoutMs=30000,
}) => {
  const { extend, release } = await simpleLock.aquire({
    retryTimeoutMs: aquireLockTimeoutMs,
  })({
    masterClient,
    name: simpleLock.lockName(key),
    ttlMs: lockTimeMs,
  })
  const extender = setInterval(
    () => extend({ ttlMs: lockTimeMs }),
    Math.round(lockTimeMs / 2),
  )

  try {
    let value = funcWoArgs()
    if (value instanceof Promise) {
      value = await value
    }

    return value
  } finally {
    clearInterval(extender)
    release()
  }
}

const get = (unmarshallFunc) => async ({ client, key }) => {
  const biData = await client.get(client.commandOptions({ returnBuffers: true }), key)
  return unmarshallFunc(biData)
}

const set = (marshallFunc) => async ({ client, key, value, ttlMs }) => {
  const dataToSet = await marshallFunc(value)
  return client.set(key, dataToSet, { PX: ttlMs })
}

const del = async ({ client, key }) => {
  return client.del(key)
}

const hSet = (marshallFunc) => async ({ client, key, fieldsValues }) => {
  const { fields, marshallings } = Object.entries(fieldsValues).reduce((accum, [f, v]) => {
    accum.fields.push(f)
    accum.marshallings.push(marshallFunc(v))
    return accum
  }, { fields: [], marshallings: [] })

  const marshalleds = await Promise.all(marshallings)

  const dataToSet = fields.reduce((accum, field, fieldIdx) => {
    accum.push(field)
    accum.push(marshalleds[fieldIdx])
    return accum
  }, [])

  return client.hSet(key, dataToSet)
}

const hDel = async ({ client, key, fields }) => {
  await client.hDel(key, ...fields)
}

const hGet = (unmarshallFunc) => async ({ client, key, field }) => {
  const biData = await client.hGet(key, field)
  return unmarshallFunc(biData)
}

const hGetAll = (unmarshallFunc) => async ({ client, key }) => {
  const keysValues = await client.hGetAll(key)
  if (keysValues == null ||
      typeof keysValues != 'object' || Object.values(keysValues).length <= 0) {
    return undefined
  }
  return Object.entries(keysValues).reduce((accum, [field, value]) => {
    accum[field] = unmarshallFunc(value)
    return accum
  }, {})
}

// need wrapped by transaction to ensure atomic
// ref: https://redis.io/docs/latest/develop/interact/transactions/#optimistic-locking-using-check-and-set
// note: if everyone race to changes FOREVER, we can not ensure EVENTUALLY the key will be set here
// in that case, use wrapWithPessimisticSimpleLock
const getOrSetWithOptimisticLock = ({ marshallFunc, unmarshallFunc }) => async ({ client, funcWoArgs, key, ttlMs }) => {
  let value = await get(unmarshallFunc)({ client, key })
  if (value != null) {
    // we don't need GET-UPDATE-SET here
    // so, it ok to not wrap GET inside WATCH
    return value
  }

  return new Promise((res, rej) => {
    client.executeIsolated(async (isolatedClient) => {
      await isolatedClient.watch(key)

      const multi = isolatedClient.multi()

      value = funcWoArgs()
      if (value instanceof Promise) {
        value = await value
      }
      await set(marshallFunc)({ client: multi, key, value, ttlMs })

      await multi.exec()

      res(value)
    }).catch(rej)
  })
}

const getOrSetWithWithPessimisticLock = ({
  marshallFunc,
  unmarshallFunc,
}) => async ({
  masterClient,
  funcWoArgs,
  key,
  ttlMs,
  lockWrapper,
}) => {
  const getOrSet = async () => {
    let value = await get(unmarshallFunc)({ client: masterClient, key })
    if (value != null) {
      return value
    }

    value = funcWoArgs()
    if (value instanceof Promise) {
      value = await value
    }
    await set(marshallFunc)({ client: masterClient, key, value, ttlMs })
    return value
  }

  return lockWrapper({
    funcWoArgs: getOrSet,
    masterClient,
    key,
  })
}

// read from replica first, if not found
// do getOrSetFromMaster
const cacheAsideFunc = ({
  marshallFunc,
  unmarshallFunc,
}) => ({
  funcWoArgs,
  key,
  ttlMs,
  lockTimeMs=5000,
  aquireLockTimeoutMs=30000,
}) => {
  return async ({ replicaClient, masterClient }) => {
    const value = await get(unmarshallFunc)({
      client: replicaClient,
      key,
    })
    if (value != null) {
      return value
    }

    const lockWrapper = ({ funcWoArgs, masterClient, key }) => {
      return wrapWithPessimisticSimpleLock({
        masterClient,
        funcWoArgs,
        key,
        lockTimeMs,
        aquireLockTimeoutMs,
      })
    }

    return getOrSetWithWithPessimisticLock({
      marshallFunc,
      unmarshallFunc,
    })({
      masterClient,
      funcWoArgs,
      key,
      ttlMs,
      lockWrapper,
    })
  }
}

// TODO: get client per key, when run in cluster mode
const createClient = ({ host = 'localhost', port = 6379, tls = false, ttl = 3600, maxEntries = 500 }) => {
  return redis.createClient({
    socket: {
      host,
      port,
      tls
    },
    RESP: 3,
    clientSideCache: {
      ttl, // Time-to-live (0 = no expiration)
      maxEntries, // Maximum entries (0 = unlimited)
      evictPolicy: 'LRU', // Eviction policy: "LRU" or "FIFO"
    },
  })
}

module.exports = {
  createClient,
  //
  get,
  set,
  del,
  getOrSetWithWithPessimisticLock,
  getOrSetWithOptimisticLock,
  hSet,
  hDel,
  hGet,
  hGetAll,
  //
  simpleLock,
  wrapWithPessimisticSimpleLock,
  cacheAsideFunc,
}
