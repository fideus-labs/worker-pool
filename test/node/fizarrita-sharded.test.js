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

test('an index entry past the shard, read by range, is invalid metadata', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, GEOMETRY)
  // Inner chunk (0,0)'s offset, in the index at the shard's end: the range
  // the index promises then runs off the shard, and comes back short.
  const shard = store.get('/data/c/0/0')
  new DataView(shard.buffer, shard.byteOffset).setBigUint64(shard.length - 68, BigInt(shard.length - 8), true)

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), (error) => {
      assert.ok(zarr.isZarritaError(error, 'InvalidMetadataError'), String(error))
      assert.match(error.message, /promises 64 bytes/)
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

const ALL_SHARDS = ['0,0', '0,1', '1,0', '1,1']

/** Open the array afresh: zarrita's own `Array` remembers shard indexes. */
const reopen = (store) => zarr.open(zarr.root(store).resolve('/data'), { kind: 'array' })

/** Parse a shard's index (at the end, with crc32c) into `(offset, length)` pairs. */
function parseIndex(shard, count) {
  const raw = shard.subarray(shard.length - (16 * count + 4), shard.length - 4)
  const crc = new DataView(shard.buffer, shard.byteOffset + shard.length - 4).getUint32(0, true)
  assert.equal(crc, crc32c(raw), 'index checksum')
  return new BigUint64Array(raw.slice().buffer)
}

/** The bytes of inner chunk `flat` in `shard`, or undefined when absent. */
function innerBytes(shard, count, flat) {
  const index = parseIndex(shard, count)
  const [offset, length] = [index[flat * 2], index[flat * 2 + 1]]
  if (offset === MISSING) return undefined
  return shard.subarray(Number(offset), Number(offset + length))
}

/** `expected` with `patch` (C order, `patchShape`) written at rows/cols from `at`. */
function withPatch(expected, shape, at, patchShape, patch) {
  const out = expected.slice()
  for (let r = 0; r < patchShape[0]; r++) {
    for (let c = 0; c < patchShape[1]; c++) {
      out[(at[0] + r) * shape[1] + at[1] + c] = patch[r * patchShape[1] + c]
    }
  }
  return out
}

test('setWorker writes a whole sharded array that zarr.get reads back', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, {
    ...GEOMETRY,
    innerCodecs: ZSTD,
    missingShards: ALL_SHARDS,
  })
  const data = { data: GEOMETRY.values, shape: [10, 12], stride: [12, 1] }

  await withPool(3, async (pool) => {
    await setWorker(arr, null, data, { pool })

    const fresh = await reopen(store)
    assert.deepEqual(Array.from((await zarr.get(fresh)).data), Array.from(GEOMETRY.values))
    const viaWorker = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(viaWorker.data), Array.from(GEOMETRY.values))
  })

  // Four shards, each indexed for 2x2 inner chunks; inner chunks beyond the
  // array's edge are absent, not written as fill.
  const present = (key) =>
    [0, 1, 2, 3].map((flat) => innerBytes(store.get(key), 4, flat) !== undefined)
  assert.deepEqual(present('/data/c/0/0'), [true, true, true, true])
  assert.deepEqual(present('/data/c/0/1'), [true, false, true, false])
  assert.deepEqual(present('/data/c/1/0'), [true, true, false, false])
  assert.deepEqual(present('/data/c/1/1'), [true, false, false, false])
})

for (const useSharedArrayBuffer of [false, true]) {
  test(`a partial write rewrites only the touched inner chunks (useSharedArrayBuffer: ${useSharedArrayBuffer})`, async () => {
    const store = new RangeStore()
    const { arr, expected } = await buildSharded(store, {
      ...GEOMETRY,
      innerCodecs: ZSTD,
      // Inner chunk (0, 1) — rows 0-3, cols 4-7 — is absent from its shard.
      missing: ['0,1'],
    })
    const before = new Map([...store].filter(([k]) => k.includes('/c/')).map(([k, v]) => [k, v.slice()]))

    // Rows 2-5, cols 5-8: shards (0,0) and (0,1); inner column 0 untouched.
    const patchShape = [4, 4]
    const patch = Int32Array.from({ length: 16 }, (_, i) => -(i + 100))
    const selection = [zarr.slice(2, 6), zarr.slice(5, 9)]
    const after = withPatch(expected, [10, 12], [2, 5], patchShape, patch)

    await withPool(2, async (pool) => {
      // A read first, so the shard indexes are remembered — and must be
      // forgotten by the write.
      assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(expected))

      await setWorker(arr, selection, { data: patch, shape: patchShape, stride: [4, 1] }, { pool, useSharedArrayBuffer })

      assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(after))
      assert.deepEqual(Array.from((await zarr.get(await reopen(store))).data), Array.from(after))
    })

    // Untouched shards are untouched.
    assert.deepEqual(store.get('/data/c/1/0'), before.get('/data/c/1/0'))
    assert.deepEqual(store.get('/data/c/1/1'), before.get('/data/c/1/1'))
    // In shard (0,0), inner column 0 — flat positions 0 and 2 — kept its bytes;
    // the absent inner chunk (0,1) — flat 1 — is now present.
    const shard = store.get('/data/c/0/0')
    const was = before.get('/data/c/0/0')
    assert.deepEqual(innerBytes(shard, 4, 0), innerBytes(was, 4, 0))
    assert.deepEqual(innerBytes(shard, 4, 2), innerBytes(was, 4, 2))
    assert.equal(innerBytes(was, 4, 1), undefined)
    assert.notEqual(innerBytes(shard, 4, 1), undefined)
  })
}

test('a scalar write to a sharded array with its index at the start', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, {
    ...GEOMETRY,
    indexCodecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
    indexLocation: 'start',
  })
  const after = expected.slice()
  for (let r = 1; r < 9; r++) after[r * 12 + 4] = 99

  await withPool(2, async (pool) => {
    await setWorker(arr, [zarr.slice(1, 9), 4], 99, { pool })
    assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(after))
  })
  // The index leads, and offsets are from the shard's start, past the index.
  const shard = store.get('/data/c/0/0')
  const index = new BigUint64Array(shard.slice(0, 64).buffer)
  assert.ok(index[0] >= 64n)
})

/** A 2-D chunk's values in row-major order, whatever its strides. */
function logical(chunk) {
  const out = []
  const [rows, cols] = chunk.shape
  const [rs, cs] = chunk.stride
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) out.push(chunk.data[r * rs + c * cs])
  return out
}

test('a partial write through transposed inner codecs', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, {
    ...GEOMETRY,
    innerCodecs: [
      { name: 'transpose', configuration: { order: [1, 0] } },
      { name: 'bytes', configuration: { endian: 'little' } },
    ],
  })
  const patch = Int32Array.from({ length: 6 }, (_, i) => 1000 + i)
  const after = withPatch(expected, [10, 12], [3, 6], [2, 3], patch)

  await withPool(2, async (pool) => {
    // The shards as built read back first, both ways. zarr.get keeps the
    // array's transposed layout; getWorker returns C order.
    assert.deepEqual(logical(await zarr.get(arr)), Array.from(expected), 'zarr.get before')
    assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(expected), 'getWorker before')
    await setWorker(arr, [zarr.slice(3, 5), zarr.slice(6, 9)], { data: patch, shape: [2, 3], stride: [3, 1] }, { pool })
    assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(after))
  })
  assert.deepEqual(logical(await zarr.get(await reopen(store))), Array.from(after))
})

test('aborting a sharded write leaves the shards still queued untouched', async () => {
  const controller = new AbortController()
  const reason = new Error('stop writing')
  let shardWrites = 0
  let armed = false
  class AbortAfterFirstShard extends RangeStore {
    set(key, value) {
      super.set(key, value)
      if (armed && key.includes('/c/')) {
        shardWrites++
        controller.abort(reason)
      }
      return this
    }
  }
  const store = new AbortAfterFirstShard()
  const { arr } = await buildSharded(store, GEOMETRY)
  const before = new Map([...store].filter(([k]) => k.includes('/c/')).map(([k, v]) => [k, v.slice()]))
  armed = true

  await withPool(1, async (pool) => {
    await assert.rejects(
      setWorker(arr, null, 5, { pool, signal: controller.signal }),
      (error) => error === reason,
    )
  })
  assert.equal(shardWrites, 1, 'only the shard written before the abort')
  const changed = [...before].filter(([k, v]) => !store.get(k).every((b, i) => b === v[i]))
  assert.equal(changed.length, 1)
})

test('a shard replaced outright is written without being read', async () => {
  const store = new RangeStore()
  const { arr, expected } = await buildSharded(store, { ...GEOMETRY, innerCodecs: ZSTD })
  // Shard (0,0) is rows 0-7, cols 0-7: four inner chunks, all covered.
  const patch = Int32Array.from({ length: 64 }, (_, i) => -i)
  const after = withPatch(expected, [10, 12], [0, 0], [8, 8], patch)

  await withPool(2, async (pool) => {
    store.gets.length = 0
    await setWorker(arr, [zarr.slice(0, 8), zarr.slice(0, 8)], { data: patch, shape: [8, 8], stride: [8, 1] }, { pool })
    assert.equal(store.gets.filter((k) => k.includes('/c/')).length, 0, 'the stored shard was not fetched')
    assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(after))
  })
  assert.deepEqual(Array.from((await zarr.get(await reopen(store))).data), Array.from(after))
})

test('a big-endian index codec is honoured on write and read', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, {
    ...GEOMETRY,
    indexCodecs: [
      { name: 'bytes', configuration: { endian: 'big' } },
      { name: 'crc32c', configuration: {} },
    ],
    missingShards: ALL_SHARDS,
  })
  const data = { data: GEOMETRY.values, shape: [10, 12], stride: [12, 1] }

  await withPool(2, async (pool) => {
    await setWorker(arr, null, data, { pool })
    assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(GEOMETRY.values))
  })
  assert.deepEqual(Array.from((await zarr.get(await reopen(store))).data), Array.from(GEOMETRY.values))
  // The first entry of shard (0,0) is offset 0, length > 0 — big-endian, so
  // its length's low byte is the last of the second uint64.
  const shard = store.get('/data/c/0/0')
  const index = shard.subarray(shard.length - 68, shard.length - 4)
  assert.deepEqual(Array.from(index.subarray(0, 8)), [0, 0, 0, 0, 0, 0, 0, 0])
  assert.notEqual(index[15], 0)
  assert.equal(index[8], 0)
})

test('a stored shard whose index points past its end is invalid metadata', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, GEOMETRY)
  // Inner chunk (0,0)'s length, in the index at the shard's end: one byte
  // more than the shard holds.
  const shard = store.get('/data/c/0/0')
  new DataView(shard.buffer, shard.byteOffset).setBigUint64(shard.length - 68 + 8, BigInt(shard.length + 1), true)

  await withPool(1, async (pool) => {
    // A partial write reads the shard to keep the rest of it.
    await assert.rejects(setWorker(arr, [0, 0], 1, { pool }), (error) => {
      assert.ok(zarr.isZarritaError(error, 'InvalidMetadataError'), String(error))
      assert.match(error.message, /past the shard's end/)
      return true
    })
  })
})

test('a stored shard shorter than its index is invalid metadata', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, GEOMETRY)
  store.set('/data/c/0/0', store.get('/data/c/0/0').subarray(0, 3))

  await withPool(1, async (pool) => {
    await assert.rejects(setWorker(arr, [0, 0], 1, { pool }), (error) =>
      zarr.isZarritaError(error, 'InvalidMetadataError') && /shorter than/.test(error.message),
    )
  })
})

test('a shard write the store rejects still forgets the remembered index', async () => {
  let armed = false
  class WriteThenFail extends RangeStore {
    set(key, value) {
      super.set(key, value)
      if (armed && key.includes('/c/')) throw new Error('lost the acknowledgement')
      return this
    }
  }
  const store = new WriteThenFail()
  const { arr, expected } = await buildSharded(store, { ...GEOMETRY, innerCodecs: ZSTD })
  armed = true
  const indexRequests = () => store.ranges.filter((r) => 'suffixLength' in r && r.key === '/data/c/0/0').length
  const patch = Int32Array.from({ length: 64 }, (_, i) => -i)
  const after = withPatch(expected, [10, 12], [0, 0], [8, 8], patch)

  await withPool(2, async (pool) => {
    await getWorker(arr, null, { pool })
    assert.equal(indexRequests(), 1)

    // The store keeps the bytes but reports failure.
    await assert.rejects(
      setWorker(arr, [zarr.slice(0, 8), zarr.slice(0, 8)], { data: patch, shape: [8, 8], stride: [8, 1] }, { pool }),
      /lost the acknowledgement/,
    )

    // The next read must not trust the old index against the new bytes.
    const result = await getWorker(arr, null, { pool })
    assert.equal(indexRequests(), 2, 'the index was fetched afresh')
    assert.deepEqual(Array.from(result.data), Array.from(after))
  })
})

test('an index codec that cannot be encoded is refused', async () => {
  const store = new RangeStore()
  const { arr } = await buildSharded(store, {
    ...GEOMETRY,
    indexCodecs: [
      { name: 'bytes', configuration: { endian: 'little' } },
      { name: 'gzip', configuration: { level: 1 } },
    ],
    missingShards: ALL_SHARDS,
  })

  await withPool(1, async (pool) => {
    // Shard (0,0) replaced outright, so nothing is read: the refusal is the
    // index encoder's.
    await assert.rejects(
      setWorker(arr, [zarr.slice(0, 8), zarr.slice(0, 8)], 1, { pool }),
      (error) =>
        zarr.isZarritaError(error, 'UnsupportedError') && /gzip/.test(error.message),
    )
  })
})
