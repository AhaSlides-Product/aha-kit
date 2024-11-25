const redis = require('redis-support-transaction')
const { generateRandomString } = require('./random')

// lock impl requirements
// - eventually can aquire
// - no deadlock
// - only lock owner can unlock
// ref: https://redis.io/docs/latest/develop/use/patterns/distributed-locks/
//
// consider use redlock when run in cluster mode
// ref: https://github.com/mike-marcacci/node-redlock
const simpleLock = {
  lockName: (name) => `simpleLock.${name}`,
  aquire: async ({ masterClient, name, ttlMs }) => {
    // secret used here to ensure ownership
    const secret = generateRandomString(8) + Date.now()
    const lockName = simpleLock.lockName(name)

    const result = await masterClient.set(lockName, secret, { PX: ttlMs, NX: true })
    if (result === null) {
      throw new Error('aquire failed')
    }

    return {
      release: () => simpleLock.release({ masterClient, lockName, secret }),
      extend: ({ ttlMs: _ttlMs }) => simpleLock.extend({ masterClient, lockName, secret, ttlMs: _ttlMs }),
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
    const res = await masterClient.eval(script, { keys: [name], arguments: [secret, ttlMs] })
    if (res === 0) {
      throw new Error('extend failed')
    }
  },
}

const wrapWithPessimisticSimpleLock = async ({ masterClient, funcWoArgs, key }) => {
  const lockTimeMs = 5000
  const { extend, release } = await simpleLock.aquire({
    masterClient,
    name: simpleLock.lockName(key),
    ttlMs: lockTimeMs,
  })
  const extender = setInterval(
    () => extend({ ttlMs: lockTimeMs }),
    Math.round(lockTimeMs / 2),
  )

  try {
    return funcWoArgs()
  } finally {
    clearInterval(extender)
    release()
  }
}

// ref: https://redis.io/docs/latest/develop/interact/transactions/#optimistic-locking-using-check-and-set
const wrapWithOptimisticLock = ({ masterClient, funcNeedClientAndKey, key }) => {
  return new Promise((res, rej) => {
    masterClient.executeIsolated(async (isolatedClient) => {
      // if key changed by other (not redis's TTL evict), transaction will abort
      // note: if everyone race to changes forever, we can not ensure key will be set
      await isolatedClient.watch(key)
      const multi = isolatedClient.multi().ping()

      res(funcNeedClientAndKey({ client: isolatedClient, key }))

      return multi.exec()
    }).catch(rej)
  })
}

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

const get = (unmarshallFunc) => async ({ client, key }) => {
  console.log('=================', { key })
  const biData = await client.get(key)
  return unmarshallFunc(biData)
}

const set = (marshallFunc) => async ({ client, key, value, ttlMs }) => {
  await client.set(key, marshallFunc(value), { PX: ttlMs })
}

const hset = (marshallFunc) => async ({ client, key, field, value}) => {
  await client.hset(key, field, marshallFunc(value))
}

const hdel = async ({ client, key, field }) => {
  await client.hdel(key, field)
}

const hget = (unmarshallFunc) => async ({ client, key, field }) => {
  const biData = await client.hget(key, field)
  return unmarshallFunc(biData)
}

const hgetall = (unmarshallFunc) => async ({ client, key, field }) => {
  const keysValues = await client.hgetall(key, field)
  return keysValues.reduce((accum, key, value) => {
    accum[key] = unmarshallFunc(value)
    return accum
  }, {})
}

// need wrapped by transaction to ensure atomic
const getOrSet = async ({ client, funcWoArgs, key, ttlMs }) => {
  let value = await get(jsonMarsh.unmarshall)({ client, key })
  if (value == null) {
    value = funcWoArgs()
    if (value instanceof Promise) {
      value = await value
    }

    await set(jsonMarsh.marshall)({ client, key, value, ttlMs })
  }
  return value
}

// read from replica first, if not found
// do getOrSetFromMaster within a transaction
const cacheAsideFunc = ({ funcWoArgs, key, ttlMs }) => {
  return async ({ replicaClient, masterClient }) => {
    const value = await get(jsonMarsh.unmarshall)({
      client: replicaClient,
      key,
    })
    if (value != null) {
      return value
    }

    const getOrSetFromMaster = ({ client, key }) => {
      return getOrSet({ client, funcWoArgs, key, ttlMs })
    }
    return wrapWithOptimisticLock({
      masterClient,
      funcNeedClientAndKey: getOrSetFromMaster,
      key,
    })
  }
}

// TODO: get client per key, when run in cluster mode
const createClient = ({ host = 'localhost', port = 6379 }) => {
  return redis.createClient({
    url: `redis://${host}:${port}`,
  })
}

module.exports = {
  get,
  set,
  hset,
  hdel,
  hget,
  hgetall,
  createClient,
  simpleLock,
  wrapWithOptimisticLock,
  wrapWithPessimisticSimpleLock,
  cacheAsideFunc,
}
