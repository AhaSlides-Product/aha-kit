const { simpleLock } = require('./redis')

describe('simpleLock', () => {
  let mockClient

  beforeEach(() => {
    // Create a mock Redis client with the methods we need
    mockClient = {
      set: jest.fn().mockResolvedValue('OK'),
      eval: jest.fn().mockResolvedValue(1),
      get: jest.fn().mockResolvedValue('mock-secret')
    }
  })

  describe('lock name generation', () => {
    test('should generate correct lock names', () => {
      expect(simpleLock.lockName('test')).toBe('simpleLock.test')
      expect(simpleLock.lockName('my-lock')).toBe('simpleLock.my-lock')
    })
  })

  describe('acquire functionality', () => {
    test('should acquire lock successfully', async () => {
      const lockName = 'test-lock'
      const ttlMs = 5000

      const lock = await simpleLock.aquire({ retryTimeoutMs: 1000 })({
        masterClient: mockClient,
        name: lockName,
        ttlMs
      })

      expect(lock).toHaveProperty('release')
      expect(lock).toHaveProperty('extend')
      expect(mockClient.set).toHaveBeenCalledWith(
        'simpleLock.test-lock',
        expect.any(String),
        { PX: 5000, NX: true }
      )
    })

    test('should handle lock acquisition failure', async () => {
      // Mock to always return null (lock already exists)
      mockClient.set.mockResolvedValue(null)

      await expect(
        simpleLock.aquire({ retryTimeoutMs: 100 })({
          masterClient: mockClient,
          name: 'test-lock',
          ttlMs: 1000
        })
      ).rejects.toThrow('aquire failed')
    })
  })

  describe('extend functionality - tests the commit fix', () => {
    test('should convert ttlMs to string when calling extend', async () => {
      const lockName = 'test-lock'
      const secret = 'test-secret'

      await simpleLock.extend({
        masterClient: mockClient,
        name: lockName,
        secret,
        ttlMs: 5000
      })

      // Verify that eval was called with ttlMs converted to string
      expect(mockClient.eval).toHaveBeenCalledWith(
        expect.any(String), // The Lua script
        {
          keys: [lockName],
          arguments: [secret, '5000'] // ttlMs should be string, not number
        }
      )
    })

    test('should handle different numeric ttlMs values', async () => {
      const testCases = [0, 1, 1000, 999999]

      for (const ttlMs of testCases) {
        mockClient.eval.mockClear()

        await simpleLock.extend({
          masterClient: mockClient,
          name: 'test-lock',
          secret: 'secret',
          ttlMs
        })

        expect(mockClient.eval).toHaveBeenCalledWith(
          expect.any(String),
          {
            keys: ['test-lock'],
            arguments: ['secret', String(ttlMs)]
          }
        )
      }
    })

    test('should throw error when extend fails', async () => {
      mockClient.eval.mockResolvedValueOnce(0) // Simulate extend failure

      await expect(
        simpleLock.extend({
          masterClient: mockClient,
          name: 'test-lock',
          secret: 'wrong-secret',
          ttlMs: 5000
        })
      ).rejects.toThrow('extend failed')
    })
  })

  describe('release functionality', () => {
    test('should call eval with correct parameters for release', async () => {
      const lockName = 'test-lock'
      const secret = 'test-secret'

      await simpleLock.release({
        masterClient: mockClient,
        name: lockName,
        secret
      })

      expect(mockClient.eval).toHaveBeenCalledWith(
        expect.stringContaining('if redis.call("get",KEYS[1]) == ARGV[1]'),
        {
          keys: [lockName],
          arguments: [secret]
        }
      )
    })
  })

  describe('integration with lock methods', () => {
    test('lock extend method should convert ttlMs to string', async () => {
      const lock = await simpleLock.aquire({ retryTimeoutMs: 1000 })({
        masterClient: mockClient,
        name: 'test-lock',
        ttlMs: 1000
      })

      mockClient.eval.mockClear()

      await lock.extend({ ttlMs: 8000 })

      // Verify the extend method on the lock object converts ttlMs to string
      expect(mockClient.eval).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          arguments: expect.arrayContaining(['8000'])
        })
      )
    })
  })
})
