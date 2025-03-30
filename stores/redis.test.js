const { GenericContainer } = require('testcontainers')
const {
  createClient,
  set, get,
  del,
  wrapWithPessimisticSimpleLock,
  getOrSetWithWithPessimisticLock, getOrSetWithOptimisticLock,
  cacheAsideFunc,
} = require('./redis')

const {
  msgpackEncDec
} = require("./encode")

const createClientWithEventsHandled = (config) => {
  const client = createClient(config)
  client.on('error', (err) => {
    console.log(err)
  }).connect()

  return client
}

describe('Redis Integration Test', () => {
  let masterClient;
  let masterContainer;
  let replicaClient;
  let replicaContainer;

  let setFunc = set(msgpackEncDec.encode)
  let getFunc = get(msgpackEncDec.decode)

  let cacheAside = cacheAsideFunc({
    marshallFunc: msgpackEncDec.encode,
    unmarshallFunc: msgpackEncDec.decode,
  })

  let getOrSetFunc = getOrSetWithWithPessimisticLock({
    marshallFunc: msgpackEncDec.encode,
    unmarshallFunc: msgpackEncDec.decode,
  })

  const sleepMs = (ms) => {
    return new Promise((res) => {
      setTimeout(res, ms)
    })
  }

  beforeAll(async () => {
    [masterContainer, replicaContainer] = await Promise.all([
      new GenericContainer('valkey/valkey:8.0.2').withExposedPorts(6379).start(),
      new GenericContainer('valkey/valkey:8.0.2').withExposedPorts(6379).start(),
    ])

    masterClient = createClientWithEventsHandled({
      host: masterContainer.getHost(),
      port: masterContainer.getMappedPort(6379),
    })
    replicaClient = createClientWithEventsHandled({
      host: replicaContainer.getHost(),
      port: replicaContainer.getMappedPort(6379),
    })

    await Promise.all([masterClient.ping(), replicaClient.ping()])
  }, 10000);

  afterAll(async () => {
    await Promise.all([
      replicaClient.quit(),
      masterClient.quit(),
    ])

    await Promise.all([
      replicaContainer.stop(),
      masterContainer.stop(),
    ])
  }, 10000);

  describe('cacheAsideFunc', () => {
    const key = "key"
    // FIXME(peter): string value not work here?!
    const value = { data: "data here" }

    beforeEach(async () => {
      await Promise.all([
        del({ client: replicaClient, key }),
        del({ client: masterClient, key }),
      ])
    })

    test('not request redis master if found from replica', async () => {
      await setFunc({ client: replicaClient, key, value, ttlMs: 1000 })

      const got = await cacheAside({
        funcWoArgs: () => sleepMs(1000),
        key,
        ttlMs: 1000,
      })({ replicaClient, masterClient })

      expect(got).toStrictEqual(value)
    })

    test('set if not found must work', async () => {
      const got = await cacheAside({
        funcWoArgs: () => value,
        key,
        ttlMs: 1000,
      })({ replicaClient, masterClient })
      expect(got).toStrictEqual(value)

      const gotFromGet = await getFunc({ client: masterClient, key })
      expect(gotFromGet).toStrictEqual(value)
    })

    describe('concurrent cacheAside', () => {
      const keyTtlMs = 10
      const getFromDbMs = 100
      const delayAfterFirstStart = Math.round(getFromDbMs / 3)

      const racingGet = async ({ aquireLockTimeoutMs }) => {
        const firstGet = cacheAside({
          client: masterClient,
          funcWoArgs: async () => {
            await sleepMs(getFromDbMs)
            return value
          },
          key, ttlMs: keyTtlMs,
          aquireLockTimeoutMs,
        }) ({ replicaClient, masterClient })

        await sleepMs(delayAfterFirstStart)

        const secondGet = cacheAside({
          client: masterClient,
          funcWoArgs: async () => {
            await sleepMs(getFromDbMs)
            return value
          },
          key, ttlMs: keyTtlMs,
          aquireLockTimeoutMs,
        })({ replicaClient, masterClient })

        return [firstGet, secondGet]
      }

      test('fetch func faster than aquireLockTimeoutMs must work', async () => {
        const [firstGet, secondGet] = await racingGet({
          aquireLockTimeoutMs: 30000,
        })
        const secondGot = await secondGet
        expect(secondGot).toStrictEqual(value)

        const firstGot = await firstGet
        expect(firstGot).toStrictEqual(value)

        const gotFromGet = await getFunc({ client: masterClient, key })
        expect(gotFromGet).toStrictEqual(value)
      })

      test('fetch func SLOWER than aquireLockTimeoutMs must work', async () => {
        const [firstGet, secondGet] = await racingGet({
          aquireLockTimeoutMs: Math.round(getFromDbMs/3),
        })

        await expect(secondGet).rejects.toThrow("aquire failed")

        const firstGot = await firstGet
        expect(firstGot).toStrictEqual(value)

        const gotFromGet = await getFunc({ client: masterClient, key })
        expect(gotFromGet).toStrictEqual(value)
      })
    })

    describe('not found at replica, getOrSet from master', () => {
      const lockWrapper = ({ funcWoArgs, masterClient, key }) => {
        return wrapWithPessimisticSimpleLock({
          masterClient,
          funcWoArgs,
          key,
          lockTimeMs: 5000,
          aquireLockTimeoutMs: 3000,
        })
      }

      test('should return if found by GET', async () => {
        await setFunc({ client: masterClient, key, value, ttlMs: 1000 })

        let beCalled = false
        const got = await getOrSetFunc({
          masterClient,
          funcWoArgs: () => { beCalled = true },
          key,
          ttlMs: 1000,
          lockWrapper,
        })

        expect(beCalled).toStrictEqual(false)
        expect(got).toStrictEqual(value)
      })

      test('should SET success if no change at middle of transaction', async () => {
        const got = await getOrSetFunc({
          masterClient,
          funcWoArgs: () => value,
          key,
          ttlMs: 1000,
          lockWrapper,
        })
        expect(got).toStrictEqual(value)

        const gotFromGet = await getFunc({ client: masterClient, key })
        expect(gotFromGet).toStrictEqual(value)
      })

      test('should err SET if has change at middle of transaction', async () => {
        const keyTtlMs = 10
        const getFromDbMs = 100
        const delayAfterFirstStart = Math.round(getFromDbMs / 3)

        let getOrSetFunc = getOrSetWithOptimisticLock({
          marshallFunc: msgpackEncDec.encode,
          unmarshallFunc: msgpackEncDec.decode,
        })

        const racing = Promise.all([
          getOrSetFunc({
            client: masterClient,
            funcWoArgs: async () => {
              await sleepMs(getFromDbMs)
              return value
            },
            key, ttlMs: keyTtlMs}),
          getOrSetFunc({
            client: masterClient,
            funcWoArgs: async () => {
              await sleepMs(delayAfterFirstStart),
              await sleepMs(getFromDbMs)
              return value
            },
            key, ttlMs: keyTtlMs}),
        ])

        await expect(racing).rejects.toThrow("One (or more) of the watched keys has been changed")

        const gotFromGet = await getFunc({ client: masterClient, key })
        expect(gotFromGet).toStrictEqual(null)
      })
    })
  })
});
