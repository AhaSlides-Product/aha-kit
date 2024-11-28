const snappy = require('snappy')

const zlib = require('zlib')
const { promisify } = require('util')

const gzipAsync = promisify(zlib.gzip)
const gunzipAsync = promisify(zlib.gunzip)

const gzipCompr = {
  compressAsync: ({ biData }) => {
    return gzipAsync(biData)
  },
  uncompressAsync: ({ biData }) => {
    return gunzipAsync(biData)
  }
}

const snappyCompr = {
  compress: ({ biData }) => {
    return snappy.compressSync(biData)
  },
  compressAsync: ({ biData }) => {
    return snappy.compress(biData)
  },
  uncompress: ({ biData }) => {
    return snappy.uncompressSync(biData)
  },
  uncompressAsync: ({ biData }) => {
    return snappy.uncompress(biData)
  }
}

module.exports = {
  snappyCompr,
  gzipCompr,
}
