/**
 * getWorker on sharded (`sharding_indexed`) arrays.
 *
 * zarrita cannot write shards, so each test builds its own: inner chunks
 * encoded with the inner codecs and concatenated, plus the `(offset, length)`
 * index. Where zarrita can read the result — index at the end, a store with
 * `getRange` — the worker read is checked against `zarr.get` as well as
 * against the values that went in.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import * as zarr from 'zarrita'

import { WorkerPool } from '../../dist/index.js'
import { getWorker, setWorker } from '../../fizarrita/dist/index.js'

async function withPool(size, fn) {
  const pool = new WorkerPool(size)
  try {
    return await fn(pool)
  } finally {
    pool.terminateWorkers()
  }
}

// ---------------------------------------------------------------------------
// Stores
// ---------------------------------------------------------------------------

/** A Map store with `getRange`, recording every request it serves. */
class RangeStore extends Map {
  gets = []
  ranges = []
  get(key, opts) {
    this.gets?.push(key)
    return super.get(key)
  }
  getRange(key, range, opts) {
    this.ranges.push({ key, ...range })
    const bytes = super.get(key)
    if (!bytes) return undefined
    if ('suffixLength' in range) {
      return bytes.subarray(bytes.length - range.suffixLength)
    }
    return bytes.subarray(range.offset, range.offset + range.length)
  }
}

/** A Map store without `getRange`, recording every `get`. */
class WholeStore extends Map {
  gets = []
  get(key, opts) {
    this.gets?.push(key)
    return super.get(key)
  }
}

// ---------------------------------------------------------------------------
// Shard building
// ---------------------------------------------------------------------------

const CRC32C_TABLE = (() => {
  const table = new Uint32Array(256)
  for (let n = 0; n < 256; n++) {
    let c = n
    for (let k = 0; k < 8; k++) c = c & 1 ? 0x82f63b78 ^ (c >>> 1) : c >>> 1
    table[n] = c
  }
  return table
})()

/** CRC-32C (Castagnoli), as the `crc32c` codec appends it. */
function crc32c(bytes) {
  let crc = 0xffffffff
  for (const byte of bytes) crc = CRC32C_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8)
  return (crc ^ 0xffffffff) >>> 0
}
assert.equal(crc32c(new TextEncoder().encode('123456789')), 0xe3069283)

const MISSING = 0xffffffffffffffffn

/** Build a codec from zarrita's registry, to encode inner chunks with. */
async function codec(name, configuration, meta) {
  const Codec = await zarr.registry.get(name)()
  return Codec.fromConfig(configuration, meta)
}

/**
 * A sharded int32 array at `/data` on `store`, holding `values` (C order,
 * `shape`), with `missing` inner chunks (by global inner-chunk coordinates,
 * as `"i,j"`) left out of their shards and `missingShards` left out of the
 * store. Returns the values the array reads as, with `fill` where a chunk is
 * missing.
 */
async function buildSharded(store, {
  shape,
  shardShape,
  chunkShape,
  values,
  fill = -1,
  innerCodecs = [{ name: 'bytes', configuration: { endian: 'little' } }],
  indexCodecs = [
    { name: 'bytes', configuration: { endian: 'little' } },
    { name: 'crc32c', configuration: {} },
  ],
  indexLocation = 'end',
  missing = [],
  missingShards = [],
}) {
  const rank = shape.length
  const grid = shardShape.map((s, i) => s / chunkShape[i])
  const shardGrid = shape.map((s, i) => Math.ceil(s / shardShape[i]))
  const chunkGrid = shape.map((s, i) => Math.ceil(s / chunkShape[i]))

  store.set(
    '/data/zarr.json',
    new TextEncoder().encode(
      JSON.stringify({
        zarr_format: 3,
        node_type: 'array',
        shape,
        data_type: 'int32',
        chunk_grid: { name: 'regular', configuration: { chunk_shape: shardShape } },
        chunk_key_encoding: { name: 'default', configuration: { separator: '/' } },
        fill_value: fill,
        attributes: {},
        codecs: [
          {
            name: 'sharding_indexed',
            configuration: {
              chunk_shape: chunkShape,
              codecs: innerCodecs,
              index_codecs: indexCodecs,
              index_location: indexLocation,
            },
          },
        ],
      }),
    ),
  )

  // The inner codec chain, built for the inner chunk shape.
  const chain = []
  for (const { name, configuration } of innerCodecs) {
    chain.push(
      await codec(name, configuration, {
        dataType: 'int32',
        shape: chunkShape,
        codecs: innerCodecs,
        fillValue: fill,
      }),
    )
  }
  const encodeInner = async (data) => {
    let out = {
      data,
      shape: chunkShape,
      stride: chunkShape.map((_, i) => chunkShape.slice(i + 1).reduce((a, b) => a * b, 1)),
    }
    for (const c of chain) out = await c.encode(out)
    return out
  }
  const chunkSize = chunkShape.reduce((a, b) => a * b, 1)

  // Iterate all coordinates of a grid, in C order.
  const coords = (g) => {
    const out = []
    const n = g.reduce((a, b) => a * b, 1)
    for (let flat = 0; flat < n; flat++) {
      const c = new Array(rank)
      let rem = flat
      for (let d = rank - 1; d >= 0; d--) {
        c[d] = rem % g[d]
        rem = Math.floor(rem / g[d])
      }
      out.push(c)
    }
    return out
  }

  const expected = Int32Array.from(values)
  const strides = shape.map((_, i) => shape.slice(i + 1).reduce((a, b) => a * b, 1))

  for (const shardCoords of coords(shardGrid)) {
    const shardKey = `/data/c/${shardCoords.join('/')}`
    if (missingShards.includes(shardCoords.join(','))) {
      // Every element of the shard reads as fill.
      for (const local of coords(grid)) {
        const chunkCoords = local.map((l, i) => shardCoords[i] * grid[i] + l)
        blank(chunkCoords)
      }
      continue
    }
    const parts = []
    const index = new BigUint64Array(grid.reduce((a, b) => a * b, 1) * 2)
    let offset = 0
    let i = 0
    for (const local of coords(grid)) {
      const chunkCoords = local.map((l, i) => shardCoords[i] * grid[i] + l)
      const inArray = chunkCoords.every((c, d) => c < chunkGrid[d])
      if (!inArray || missing.includes(chunkCoords.join(','))) {
        index[i * 2] = MISSING
        index[i * 2 + 1] = MISSING
        blank(chunkCoords)
      } else {
        // The full inner chunk, padded with fill beyond the array's edge.
        const data = new Int32Array(chunkSize).fill(fill)
        for (const local of coords(chunkShape)) {
          const el = local.map((l, d) => chunkCoords[d] * chunkShape[d] + l)
          if (el.every((e, d) => e < shape[d])) {
            const flatIn = el.reduce((acc, e, d) => acc + e * strides[d], 0)
            const flatChunk = local.reduce(
              (acc, l, d) => acc + l * chunkShape.slice(d + 1).reduce((a, b) => a * b, 1),
              0,
            )
            data[flatChunk] = values[flatIn]
          }
        }
        const bytes = await encodeInner(data)
        parts.push(bytes)
        index[i * 2] = BigInt(offset)
        index[i * 2 + 1] = BigInt(bytes.length)
        offset += bytes.length
      }
      i++
    }
    // Encode the index: raw little-endian uint64s, then crc32c if asked for.
    let indexBytes = new Uint8Array(index.buffer.slice(0))
    if (indexCodecs.some((c) => c.name === 'crc32c')) {
      const withCrc = new Uint8Array(indexBytes.length + 4)
      withCrc.set(indexBytes)
      new DataView(withCrc.buffer).setUint32(indexBytes.length, crc32c(indexBytes), true)
      indexBytes = withCrc
    }
    if (indexLocation === 'start') {
      // Offsets are relative to the shard start, so they follow the index.
      for (let k = 0; k < index.length; k += 2) {
        if (index[k] !== MISSING) index[k] += BigInt(indexBytes.length)
      }
      indexBytes = new Uint8Array(index.buffer.slice(0))
      if (indexCodecs.some((c) => c.name === 'crc32c')) {
        const withCrc = new Uint8Array(indexBytes.length + 4)
        withCrc.set(indexBytes)
        new DataView(withCrc.buffer).setUint32(indexBytes.length, crc32c(indexBytes), true)
        indexBytes = withCrc
      }
    }
    const shard = new Uint8Array(offset + indexBytes.length)
    let at = indexLocation === 'start' ? indexBytes.length : 0
    for (const part of parts) {
      shard.set(part, at)
      at += part.length
    }
    shard.set(indexBytes, indexLocation === 'start' ? 0 : offset)
    store.set(shardKey, shard)
  }

  function blank(chunkCoords) {
    for (const local of coords(chunkShape)) {
      const el = local.map((l, d) => chunkCoords[d] * chunkShape[d] + l)
      if (el.every((e, d) => e < shape[d])) {
        expected[el.reduce((acc, e, d) => acc + e * strides[d], 0)] = fill
      }
    }
  }

  const arr = await zarr.open(zarr.root(store).resolve('/data'), { kind: 'array' })
  return { arr, expected }
}

/** 10x12 over 8x8 shards of 4x4 inner chunks: edge shards, edge inner chunks. */
const GEOMETRY = {
  shape: [10, 12],
  shardShape: [8, 8],
  chunkShape: [4, 4],
  values: Int32Array.from({ length: 120 }, (_, i) => i * 3 + 1),
}

const ZSTD = [
  { name: 'bytes', configuration: { endian: 'little' } },
  { name: 'zstd', configuration: { level: 1 } },
]

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

test('reads a sharded array as zarr.get does', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, {
    ...GEOMETRY,
    innerCodecs: ZSTD,
    // An interior inner chunk left out of its shard, and a whole shard absent.
    missing: ['0,1'],
    missingShards: ['1,1'],
  })
  assert.deepEqual(arr.chunks, [4, 4], 'zarrita exposes the inner chunk shape')

  await withPool(2, async (pool) => {
    for (const selection of [
      null,
      [zarr.slice(2, 9), zarr.slice(3, 11)],
      [7, null],
      [zarr.slice(0, 10, 3), 5],
    ]) {
      const viaZarr = await zarr.get(arr, selection)
      const viaWorker = await getWorker(arr, selection, { pool })
      assert.deepEqual(viaWorker.shape, viaZarr.shape, JSON.stringify(selection))
      assert.deepEqual(
        Array.from(viaWorker.data),
        Array.from(viaZarr.data),
        JSON.stringify(selection),
      )
    }
    const full = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(full.data), Array.from(expected))
  })
})

test('fetches each shard index once per store, and each inner chunk by range', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, GEOMETRY)

  await withPool(2, async (pool) => {
    store.ranges.length = 0
    const first = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(first.data), Array.from(expected))

    const suffix = store.ranges.filter((r) => 'suffixLength' in r)
    const inner = store.ranges.filter((r) => 'offset' in r)
    // 2x2 shards, 20 bytes of index each (2x2 pairs of uint64 + crc32c).
    assert.equal(suffix.length, 4, 'one index request per shard')
    assert.ok(suffix.every((r) => r.suffixLength === 4 * 16 + 4))
    // 3x3 inner chunks lie within the 10x12 array.
    assert.equal(inner.length, 9, 'one range request per inner chunk present')
    assert.equal(store.gets.filter((k) => k.includes('/c/')).length, 0, 'no whole-shard reads')

    store.ranges.length = 0
    const second = await getWorker(arr, [zarr.slice(0, 4), null], { pool })
    assert.deepEqual(Array.from(second.data), Array.from(expected.subarray(0, 48)))
    assert.equal(
      store.ranges.filter((r) => 'suffixLength' in r).length,
      0,
      'the indexes are remembered across reads',
    )
    assert.equal(store.ranges.filter((r) => 'offset' in r).length, 3)
  })
})

test('a store without getRange cannot hold a sharded array, for zarrita either', async () => {
  // zarrita refuses at `open`, so no such array can reach getWorker at all.
  const store = new WholeStore()
  await assert.rejects(buildSharded(store, GEOMETRY), (error) =>
    zarr.isZarritaError(error, 'UnsupportedError'),
  )
})

test('an index at the start of the shard, without crc32c', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, {
    ...GEOMETRY,
    indexCodecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
    indexLocation: 'start',
    missing: ['1,0'],
  })

  await withPool(2, async (pool) => {
    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(result.data), Array.from(expected))
    const indexRequests = store.ranges.filter((r) => r.offset === 0 && r.length === 4 * 16)
    assert.equal(indexRequests.length, 4, 'index read from offset 0, 64 bytes')
  })
})

test('SharedArrayBuffer output and a chunk cache work on a sharded array', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, { ...GEOMETRY, innerCodecs: ZSTD })

  await withPool(2, async (pool) => {
    const shared = await getWorker(arr, null, { pool, useSharedArrayBuffer: true })
    assert.ok(shared.data.buffer instanceof SharedArrayBuffer)
    assert.deepEqual(Array.from(shared.data), Array.from(expected))

    const cache = new Map()
    const cached = await getWorker(arr, null, { pool, cache })
    assert.deepEqual(Array.from(cached.data), Array.from(expected))
    assert.equal(cache.size, 9, 'one entry per inner chunk')

    store.ranges.length = 0
    const again = await getWorker(arr, null, { pool, cache })
    assert.deepEqual(Array.from(again.data), Array.from(expected))
    assert.equal(store.ranges.length, 0, 'served entirely from the chunk cache')
  })
})

test('a sharded read can be aborted, and the next read is unaffected', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, GEOMETRY)

  await withPool(1, async (pool) => {
    const controller = new AbortController()
    const reason = new Error('moved on')
    controller.abort(reason)
    await assert.rejects(
      getWorker(arr, null, { pool, signal: controller.signal }),
      (error) => error === reason,
    )
    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(result.data), Array.from(expected))
  })
})

test('a corrupt inner chunk is reported against its shard', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, { ...GEOMETRY, innerCodecs: ZSTD })
  // Scribble over the first inner chunk's bytes; the index still points at it.
  store.get('/data/c/0/0').fill(7, 0, 8)

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), (error) => {
      assert.ok(zarr.isZarritaError(error, 'CodecPipelineError'), String(error))
      assert.equal(error.codec, 'zstd')
      assert.equal(error.chunkPath, '/data/c/0/0')
      return true
    })
  })
})

test('an index entry beyond safe-integer range is invalid metadata', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, GEOMETRY)
  // The index is the last 64 + 4 bytes of the shard; make inner chunk (0, 0)'s
  // offset 2^60 — not representable exactly as a number.
  const shard = store.get('/data/c/0/0')
  new DataView(shard.buffer, shard.byteOffset).setBigUint64(shard.length - 68, 1n << 60n, true)

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), (error) => {
      assert.ok(zarr.isZarritaError(error, 'InvalidMetadataError'), String(error))
      assert.match(error.message, /out of range/)
      return true
    })
  })
})

test('a shard shape the inner chunks do not divide is invalid metadata', async () => {
  const store = new RangeStore()
  await buildSharded(store, { ...GEOMETRY, shardShape: [8, 8], chunkShape: [4, 4] })
  const meta = JSON.parse(new TextDecoder().decode(store.get('/data/zarr.json')))
  meta.codecs[0].configuration.chunk_shape = [3, 4]
  store.set('/data/zarr.json', new TextEncoder().encode(JSON.stringify(meta)))
  const arr = await zarr.open(zarr.root(store).resolve('/data'), { kind: 'array' })

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), (error) =>
      zarr.isZarritaError(error, 'InvalidMetadataError'),
    )
  })
})

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

test('setWorker refuses a sharded array, as zarr.set does', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, GEOMETRY)
  const before = store.get('/data/c/0/0').slice()

  await assert.rejects(zarr.set(arr, null, 1), (error) =>
    zarr.isZarritaError(error, 'UnsupportedError'),
  )
  await withPool(1, async (pool) => {
    await assert.rejects(setWorker(arr, null, 1, { pool }), (error) =>
      zarr.isZarritaError(error, 'UnsupportedError'),
    )
  })
  assert.deepEqual(store.get('/data/c/0/0'), before, 'nothing was written')
})
