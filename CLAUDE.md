# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Aha-kit is a Node.js caching library that provides multi-layered caching with both in-memory (LRU) and Redis stores. It implements cache-aside and read-through patterns with data compression and serialization.

## Development Commands

- **Test**: `npm test` (runs Jest tests)
- **Release**: `npm run release` (publishes to GitHub packages registry)
- **Release with version bump**: `npm run release-with-bump-version` (uses standard-version, pushes tags, and publishes)

## Architecture

The library exports a unified `stores` module from `stores/index.js` that provides:

### Core Components

1. **Multi-layer caching system**: Combines in-app LRU cache with Redis for performance
2. **Data marshalling**: Uses msgpack + snappy compression for efficient Redis storage
3. **Cache patterns**: Implements cache-aside and read-through patterns
4. **Distributed locking**: Redis-based simple lock implementation with retry logic

### Key Modules

- `stores/redis.js`: Redis client creation, cache-aside functions, distributed locking
- `stores/inAppLru.js`: LRU cache implementation using lru-cache library
- `stores/encode.js`: msgpack encoding/decoding utilities
- `stores/compress.js`: Snappy compression utilities
- `stores/promise.js`: Promise detection utilities
- `stores/retry.js`: Backoff retry mechanism
- `stores/hash.js`: Hashing utilities
- `stores/random.js`: Random string generation

### Main API

The `create()` function returns cache functions that coordinate between layers:

- `inAppCacheReadThrough`: In-memory only caching
- `redisCacheReadThrough`: Multi-layer (in-app + Redis) caching
- `redisCacheOnlyReadThrough`: Redis-only caching
- `redisCacheWrite`: Direct Redis write operations

## Testing

- Primary test file: `stores/redis.test.js`
- Uses Jest framework
- Includes Redis mock for testing

## Publishing

- Published to GitHub packages registry as `@ahaslides-product/aha-kit`
- Manual semver versioning in package.json required before release
- Uses standard-version for changelog generation

## Dependencies

- **redis-support-transaction**: Custom Redis client with transaction support
- **lru-cache**: In-memory LRU cache implementation
- **msgpack-lite**: Binary serialization
- **snappy**: Compression library
- **lodash**: Utility functions