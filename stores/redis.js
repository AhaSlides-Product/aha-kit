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
const getOrSet = ({ marshallFunc, unmarshallFunc }) => async ({ client, funcWoArgs, key, ttlMs }) => {
  let value = await get(unmarshallFunc)({ client, key })
  if (value == null) {
    value = funcWoArgs()
    if (value instanceof Promise) {
      value = await value
    }

    await set(marshallFunc)({ client, key, value, ttlMs })
  }
  return value
}

// read from replica first, if not found
// do getOrSetFromMaster within a transaction
const cacheAsideFunc = ({ marshallFunc, unmarshallFunc }) => ({ funcWoArgs, key, ttlMs }) => {
  return async ({ replicaClient, masterClient }) => {
    const value = await get(unmarshallFunc)({
      client: replicaClient,
      key,
    })
    if (value != null) {
      return value
    }

    const getOrSetFromMaster = ({ client, key }) => {
      return getOrSet({
        marshallFunc,
        unmarshallFunc,
      })({ client, funcWoArgs, key, ttlMs })
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
  createClient,
  //
  get,
  set,
  del,
  getOrSet,
  hSet,
  hDel,
  hGet,
  hGetAll,
  //
  simpleLock,
  wrapWithOptimisticLock,
  wrapWithPessimisticSimpleLock,
  cacheAsideFunc,
}
