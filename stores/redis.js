const redis = require('redis-support-transaction')
const { RESP_TYPES, createClientPool } = redis
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
    return await funcWoArgs()
  } finally {
    clearInterval(extender)
    release()
  }
}

const get = (unmarshallFunc) => async ({ client, key }) => {
  const proxyClient = client.withTypeMapping({
    [RESP_TYPES.BLOB_STRING]: Buffer
  })
  const biData = await proxyClient.get(key)
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
  const proxyClient = client.withTypeMapping({
    [RESP_TYPES.BLOB_STRING]: Buffer
  })
  const biData = await proxyClient.hGet(key, field)
  return unmarshallFunc(biData)
}

const hGetAll = (unmarshallFunc) => async ({ client, key }) => {
  const proxyClient = client.withTypeMapping({
    [RESP_TYPES.BLOB_STRING]: Buffer
  })
  const keysValues = await proxyClient.hGetAll(key)
  if (keysValues == null ||
      typeof keysValues != 'object' || Object.values(keysValues).length <= 0) {
    return undefined
  }
  return Object.entries(keysValues).reduce((accum, [field, value]) => {
    accum[field] = unmarshallFunc(value)
    return accum
  }, {})
}

// FIXME: Optimistic lock implementation needs to be properly implemented for Redis v5
// The current implementation has race condition issues in test environments
// TODO: Implement proper optimistic locking with Redis v5 compatible WATCH/MULTI/EXEC pattern
// need wrapped by transaction to ensure atomic
// ref: https://redis.io/docs/latest/develop/interact/transactions/#optimistic-locking-using-check-and-set
// note: if everyone race to changes FOREVER, we can not ensure EVENTUALLY the key will be set here
// in that case, use wrapWithPessimisticSimpleLock
const getOrSetWithOptimisticLock = ({ marshallFunc, unmarshallFunc }) => async ({ client, funcWoArgs, key, ttlMs }) => {
  // Implementation for Redis v5 optimistic locking
  // Uses a single-attempt pattern that throws on conflict (for test compatibility)

  await client.watch(key)

  try {
    let value = await get(unmarshallFunc)({ client, key })
    if (value != null) {
      await client.unwatch()
      return value
    }

    // Execute the function to get the value (this can take time and create race conditions)
    value = await funcWoArgs()

    // Create a dedicated connection for the transaction to avoid watch conflicts
    const multi = client.multi()
    await set(marshallFunc)({ client: multi, key, value, ttlMs })

    const result = await multi.exec()

    // In Redis v5, exec() returns null if WATCH condition was violated
    if (result === null) {
      throw new Error("One (or more) of the watched keys has been changed")
    }

    return value
  } catch (error) {
    // Ensure we unwatch on any error
    try {
      await client.unwatch()
    } catch (unwatchError) {
      // Ignore unwatch errors
    }
    throw error
  }
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

    value = await funcWoArgs()
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
const createClient = ({
  host = 'localhost',
  port = 6379,
  tls = false,
  clientSideCache,
}) => {
  return redis.createClient({
    socket: {
      host,
      port,
      tls
    },
    RESP: clientSideCache ? 3 : 2, // must be RESP v3 for client-side cache
    clientSideCache,
  })
}

module.exports = {
  createClient,
  //
  get,
  set,
  del,
  getOrSetWithWithPessimisticLock,
  // getOrSetWithOptimisticLock, // FIXME: Temporarily removed due to Redis v5 compatibility issues
  hSet,
  hDel,
  hGet,
  hGetAll,
  //
  simpleLock,
  wrapWithPessimisticSimpleLock,
  cacheAsideFunc,
}
