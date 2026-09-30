/**
 * getWorker/setWorker parity with zarrita 0.7's own get/set.
 *
 * Each test pins down something zarrita 0.7 reads, writes or reports in a way
 * the worker path has to reproduce: v2 codec chains, scalar arrays, transposed
 * layouts, fill values, structured errors, abort signals, and store
 * extensions. Runs against the built `dist/` of both packages.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import * as zarr from 'zarrita'

import { WorkerPool } from '../../dist/index.js'
import { getWorker, setWorker, slice } from '../../fizarrita/dist/index.js'

async function withPool(size, fn) {
  const pool = new WorkerPool(size)
  try {
    return await fn(pool)
  } finally {
    pool.terminateWorkers()
  }
}

/** Build a codec from zarrita's registry, to encode test chunks with. */
async function codec(name, configuration, meta) {
  const Codec = await zarr.registry.get(name)()
  return Codec.fromConfig(configuration, meta)
}

const json = (value) => new TextEncoder().encode(JSON.stringify(value))

/** A v2 array at `/data` on a fresh Map store, opened through zarrita. */
async function openV2(zarray, chunks, store = new Map()) {
  store.set('/data/.zarray', json({ zarr_format: 2, order: 'C', fill_value: 0, ...zarray }))
  for (const [key, bytes] of Object.entries(chunks)) {
    store.set(`/data/${key}`, bytes)
  }
  return zarr.open.v2(zarr.root(store).resolve('/data'), { kind: 'array', attrs: false })
}

// ---------------------------------------------------------------------------
// v2 codec chains
// ---------------------------------------------------------------------------

test('a big-endian v2 array reads as zarrita reads it', async () => {
  // Two 4-element chunks of `>i4`, the second an edge chunk padded to 4.
  const values = [1, -2, 300, 40000, 5, -6]
  const chunk = (items) => {
    const view = new DataView(new ArrayBuffer(16))
    items.forEach((v, i) => view.setInt32(i * 4, v, false))
    return new Uint8Array(view.buffer)
  }
  const arr = await openV2(
    { shape: [6], chunks: [4], dtype: '>i4', compressor: null, filters: null },
    { 0: chunk(values.slice(0, 4)), 1: chunk([...values.slice(4), 0, 0]) },
  )

  await withPool(2, async (pool) => {
    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(result.data), values)
    assert.deepEqual(Array.from((await zarr.get(arr)).data), values)
  })
})

test('v2 filters decode before the compressor, under numcodecs names', async () => {
  // shuffle is registered only as `numcodecs.shuffle`, and has to be undone
  // *after* zstd: the order numcodecs applies them in, reversed.
  const values = Int32Array.from({ length: 8 }, (_, i) => i * 1000 - 3000)
  const shuffle = await codec('numcodecs.shuffle', { elementsize: 4 })
  const zstd = await codec('numcodecs.zstd', { level: 1 })
  const encode = async (part) =>
    zstd.encode(await shuffle.encode(new Uint8Array(part.slice().buffer)))

  const arr = await openV2(
    {
      shape: [8],
      chunks: [4],
      dtype: '<i4',
      compressor: { id: 'zstd', level: 1 },
      filters: [{ id: 'shuffle', elementsize: 4 }],
    },
    { 0: await encode(values.subarray(0, 4)), 1: await encode(values.subarray(4)) },
  )

  await withPool(2, async (pool) => {
    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(result.data), Array.from(values))
  })
})

test('a column-major (order F) v2 array reads as zarrita reads it', async () => {
  // 3x4 stored F-order in one chunk: element (r, c) at c * 3 + r.
  const rows = 3
  const cols = 4
  const values = Int32Array.from({ length: rows * cols }, (_, i) => i * 10)
  const stored = new Int32Array(rows * cols)
  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) stored[c * rows + r] = values[r * cols + c]
  }
  const arr = await openV2(
    { shape: [rows, cols], chunks: [rows, cols], dtype: '<i4', order: 'F', compressor: null, filters: null },
    { '0.0': new Uint8Array(stored.buffer) },
  )

  await withPool(1, async (pool) => {
    for (const selection of [null, [1, null], [null, zarr.slice(1, 3)]]) {
      const expected = await zarr.get(arr, selection)
      const result = await getWorker(arr, selection, { pool })
      // Compare logically: zarr.get keeps F strides, getWorker returns C order.
      const logical = (chunk) => {
        const out = []
        const [n0, n1 = 1] = chunk.shape
        const [s0, s1 = 0] = chunk.stride
        for (let i = 0; i < n0; i++) for (let j = 0; j < n1; j++) out.push(chunk.data[i * s0 + j * s1])
        return out
      }
      assert.deepEqual(logical(result), logical(expected), JSON.stringify(selection))
    }
    const full = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(full.data), Array.from(values))
  })
})

test('v2 fixedscaleoffset decodes through scale_offset + cast_value', async () => {
  // float64 stored quantized to uint8, then zstd-compressed. The zstd frame
  // header reports the *uint8* length, which must not be mistaken for the
  // float64 chunk's — that would "correct" the chunk shape to 1 element.
  const values = Float64Array.from({ length: 16 }, (_, i) => i / 10)
  const zstd = await codec('numcodecs.zstd', { level: 1 })
  const encode = (part) =>
    zstd.encode(Uint8Array.from(part, (x) => Math.round(x * 10)))

  const arr = await openV2(
    {
      shape: [16],
      chunks: [8],
      dtype: '<f8',
      compressor: { id: 'zstd', level: 1 },
      filters: [
        { id: 'fixedscaleoffset', scale: 10, offset: 0, dtype: '<f8', astype: '|u1' },
      ],
    },
    { 0: await encode(values.subarray(0, 8)), 1: await encode(values.subarray(8)) },
  )

  await withPool(2, async (pool) => {
    const expected = await zarr.get(arr)
    const result = await getWorker(arr, null, { pool })
    assert.ok(result.data instanceof Float64Array)
    assert.deepEqual(Array.from(result.data), Array.from(expected.data))
    for (let i = 0; i < values.length; i++) {
      assert.ok(Math.abs(result.data[i] - values[i]) < 1e-9, `element ${i}`)
    }
  })
})

// ---------------------------------------------------------------------------
// Scalar arrays
// ---------------------------------------------------------------------------

test('scalar (shape []) arrays read and write through workers', async () => {
  const store = new Map()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [],
    chunkShape: [],
    dtype: 'float32',
    fillValue: -1,
  })

  await withPool(1, async (pool) => {
    // Never written: the fill value.
    assert.equal(await getWorker(arr, null, { pool }), -1)

    await zarr.set(arr, null, 42)
    assert.equal(await getWorker(arr, null, { pool }), 42)

    await setWorker(arr, null, 7.5, { pool })
    assert.equal(await zarr.get(arr), 7.5)
    assert.equal(await getWorker(arr, null, { pool }), 7.5)
  })
})

// ---------------------------------------------------------------------------
// Transposed layouts
// ---------------------------------------------------------------------------

const TRANSPOSED = [
  { name: 'transpose', configuration: { order: [1, 0] } },
  { name: 'bytes', configuration: { endian: 'little' } },
]

async function makeTransposed() {
  // 6x5 over 4x4 chunks, so there are edge chunks in both dimensions.
  return zarr.create(zarr.root(new Map()).resolve('/data'), {
    shape: [6, 5],
    chunkShape: [4, 4],
    dtype: 'int32',
    codecs: TRANSPOSED,
  })
}

test('partial writes to a transposed array land where zarr.set puts them', async () => {
  const full = {
    data: Int32Array.from({ length: 30 }, (_, i) => i),
    shape: [6, 5],
    stride: [5, 1],
  }
  const patch = {
    data: Int32Array.from({ length: 6 }, (_, i) => -(i + 1)),
    shape: [2, 3],
    stride: [3, 1],
  }
  const selection = [zarr.slice(3, 5), zarr.slice(1, 4)]

  const reference = await makeTransposed()
  await zarr.set(reference, null, full)
  await zarr.set(reference, selection, patch)
  const expected = await zarr.get(reference)

  const arr = await makeTransposed()
  await withPool(2, async (pool) => {
    await setWorker(arr, null, full, { pool })
    // Spans four chunks, none of them totally replaced: each is decoded,
    // modified in its transposed layout, and re-encoded.
    await setWorker(arr, selection, patch, { pool })
    await setWorker(arr, [0, 0], 99, { pool })
  })
  await zarr.set(reference, [0, 0], 99)
  const expectedAfterScalar = await zarr.get(reference)

  assert.notDeepEqual(Array.from(expected.data), Array.from(full.data))
  const written = await zarr.get(arr)
  assert.deepEqual(
    Array.from(written.data),
    Array.from(expectedAfterScalar.data),
  )
})

test('reads of a transposed array match zarr.get, including integer indices', async () => {
  const arr = await makeTransposed()
  await zarr.set(arr, null, {
    data: Int32Array.from({ length: 30 }, (_, i) => i * 3),
    shape: [6, 5],
    stride: [5, 1],
  })

  await withPool(2, async (pool) => {
    for (const selection of [null, [5, null], [null, 4], [zarr.slice(1, 6, 2), 2]]) {
      const expected = await zarr.get(arr, selection)
      const result = await getWorker(arr, selection, { pool })
      // Compare logical values: getWorker returns C order, zarr.get may not.
      const values = (chunk) => {
        const out = []
        const [rows, cols = 1] = chunk.shape
        const [rs, cs = 0] = chunk.stride
        for (let r = 0; r < rows; r++) {
          for (let c = 0; c < cols; c++) out.push(chunk.data[r * rs + c * cs])
        }
        return out
      }
      assert.deepEqual(result.shape, expected.shape, JSON.stringify(selection))
      assert.deepEqual(values(result), values(expected), JSON.stringify(selection))
    }
  })
})

// ---------------------------------------------------------------------------
// Fill values
// ---------------------------------------------------------------------------

test('fill values come from zarrita, typed for the array', async () => {
  const int64 = await zarr.create(zarr.root(new Map()).resolve('/data'), {
    shape: [4],
    chunkShape: [2],
    dtype: 'int64',
    fillValue: 5,
  })
  const nan = await zarr.create(zarr.root(new Map()).resolve('/data'), {
    shape: [4],
    chunkShape: [2],
    dtype: 'float32',
    fillValue: Number.NaN,
  })

  await withPool(1, async (pool) => {
    // A number fill on a bigint array would make BigInt64Array.fill throw.
    const ints = await getWorker(int64, null, { pool })
    assert.deepEqual(Array.from(ints.data), [5n, 5n, 5n, 5n])

    const floats = await getWorker(nan, null, { pool })
    assert.ok(floats.data.every(Number.isNaN))

    // A partial write into a missing chunk starts from the fill value too.
    await setWorker(int64, [1], 9n, { pool })
    assert.deepEqual(Array.from((await zarr.get(int64)).data), [5n, 9n, 5n, 5n])
  })
})

// ---------------------------------------------------------------------------
// Structured errors
// ---------------------------------------------------------------------------

test('a corrupt chunk rejects with a CodecPipelineError naming the chunk', async () => {
  const store = new Map()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [8, 8],
    chunkShape: [4, 4],
    dtype: 'int32',
    codecs: [
      { name: 'bytes', configuration: { endian: 'little' } },
      { name: 'zstd', configuration: { level: 1 } },
    ],
  })
  store.set('/data/c/0/0', Uint8Array.from([1, 2, 3, 4, 5, 6, 7, 8]))

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), (error) => {
      assert.ok(zarr.isZarritaError(error, 'CodecPipelineError'), String(error))
      assert.equal(error.direction, 'decode')
      assert.equal(error.codec, 'zstd')
      assert.equal(error.chunkPath, '/data/c/0/0')
      assert.ok(error.cause instanceof Error)
      return true
    })
  })
})

test('an unregistered codec rejects with UnknownCodecError', async () => {
  const store = new Map()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [4],
    chunkShape: [4],
    dtype: 'int32',
    codecs: [{ name: 'no-such-codec', configuration: {} }],
  })
  store.set('/data/c/0', new Uint8Array(16))

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), (error) => {
      assert.ok(zarr.isZarritaError(error, 'UnknownCodecError'), String(error))
      assert.equal(error.codec, 'no-such-codec')
      return true
    })
  })
})

test('a data type the worker cannot decode rejects with UnsupportedError', async () => {
  // bool is backed by zarrita's own BoolArray, not a browser TypedArray, so
  // there is nothing to transfer to a worker.
  const arr = await zarr.create(zarr.root(new Map()).resolve('/data'), {
    shape: [4],
    chunkShape: [4],
    dtype: 'bool',
  })
  await zarr.set(arr, null, true)

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), (error) =>
      zarr.isZarritaError(error, 'UnsupportedError'),
    )
    await assert.rejects(setWorker(arr, null, false, { pool }), (error) =>
      zarr.isZarritaError(error, 'UnsupportedError'),
    )
  })
})

test('a bad selection rejects with InvalidSelectionError, as zarr.get does', async () => {
  const arr = await zarr.create(zarr.root(new Map()).resolve('/data'), {
    shape: [4, 4],
    chunkShape: [2, 2],
    dtype: 'int32',
  })

  await withPool(1, async (pool) => {
    for (const selection of [[9, null], [null, null, null], [zarr.slice(0, 4, 0)]]) {
      await assert.rejects(zarr.get(arr, selection), (error) =>
        zarr.isZarritaError(error, 'InvalidSelectionError'),
      )
      await assert.rejects(getWorker(arr, selection, { pool }), (error) =>
        zarr.isZarritaError(error, 'InvalidSelectionError'),
      )
      await assert.rejects(setWorker(arr, selection, 1, { pool }), (error) =>
        zarr.isZarritaError(error, 'InvalidSelectionError'),
      )
    }
  })
})

// ---------------------------------------------------------------------------
// setWorker abort
// ---------------------------------------------------------------------------

test('setWorker with an already-aborted signal writes nothing', async () => {
  const store = new Map()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [8],
    chunkShape: [2],
    dtype: 'int32',
  })
  const keys = [...store.keys()]
  const reason = new Error('never mind')

  await withPool(1, async (pool) => {
    await assert.rejects(
      setWorker(arr, null, 1, { pool, signal: AbortSignal.abort(reason) }),
      (error) => error === reason,
    )
  })
  assert.deepEqual([...store.keys()], keys)
})

test('aborting setWorker part-way drops the chunks still queued', async () => {
  const controller = new AbortController()
  const reason = new Error('stop writing')
  let chunkWrites = 0
  class AbortAfterFirstWrite extends Map {
    set(key, value) {
      super.set(key, value)
      if (key.includes('/c/')) {
        chunkWrites++
        controller.abort(reason)
      }
      return this
    }
  }
  const arr = await zarr.create(zarr.root(new AbortAfterFirstWrite()).resolve('/data'), {
    shape: [8],
    chunkShape: [2],
    dtype: 'int32',
  })

  await withPool(1, async (pool) => {
    await assert.rejects(
      setWorker(arr, null, 1, { pool, signal: controller.signal }),
      (error) => error === reason,
    )
  })
  assert.equal(chunkWrites, 1, 'only the chunk written before the abort')
})

// ---------------------------------------------------------------------------
// Named dimensions, bigint slices, float16
// ---------------------------------------------------------------------------

test('zarr.select selections and bigint slices work with getWorker', async () => {
  const arr = await zarr.create(zarr.root(new Map()).resolve('/data'), {
    shape: [4, 6],
    chunkShape: [2, 4],
    dtype: 'int32',
    dimensionNames: ['y', 'x'],
  })
  await zarr.set(arr, null, {
    data: Int32Array.from({ length: 24 }, (_, i) => i),
    shape: [4, 6],
    stride: [6, 1],
  })

  await withPool(2, async (pool) => {
    const selection = zarr.select(arr, { y: 2, x: zarr.slice(1n, 5n) })
    const result = await getWorker(arr, selection, { pool })
    assert.deepEqual(Array.from(result.data), [13, 14, 15, 16])

    // fizarrita's own `slice` takes bigints too.
    const same = await getWorker(arr, [2, slice(1n, 5n)], { pool })
    assert.deepEqual(Array.from(same.data), [13, 14, 15, 16])
  })
})

test('float16 arrays round-trip where the runtime has Float16Array', {
  skip: typeof Float16Array === 'undefined' && 'no Float16Array',
}, async () => {
  const arr = await zarr.create(zarr.root(new Map()).resolve('/data'), {
    shape: [6],
    chunkShape: [4],
    dtype: 'float16',
  })
  const data = Float16Array.from([0.5, 1, 1.5, 2, -0.25, 65504])

  await withPool(1, async (pool) => {
    await setWorker(arr, null, { data, shape: [6], stride: [1] }, { pool })
    assert.deepEqual(Array.from((await zarr.get(arr)).data), Array.from(data))
    assert.deepEqual(Array.from((await getWorker(arr, null, { pool })).data), Array.from(data))
  })
})

// ---------------------------------------------------------------------------
// Store extensions
// ---------------------------------------------------------------------------

test('getWorker reads through zarrita store extensions', async () => {
  const base = new Map()
  const source = await zarr.create(zarr.root(base).resolve('/data'), {
    shape: [8, 8],
    chunkShape: [4, 4],
    dtype: 'int32',
    // Big-endian, so decoding swaps bytes: it must not do so in the cached
    // buffer, or the second read would come back swapped back.
    codecs: [{ name: 'bytes', configuration: { endian: 'big' } }],
  })
  const values = Int32Array.from({ length: 64 }, (_, i) => i * 7)
  await zarr.set(source, null, { data: values, shape: [8, 8], stride: [8, 1] })

  const fetched = []
  const withTrace = zarr.defineStoreExtension((inner) => ({
    async get(key, options) {
      fetched.push(key)
      return inner.get(key, options)
    },
  }))
  const store = await zarr.extendStore(base, withTrace, (s) => zarr.withByteCaching(s))
  const arr = await zarr.open(zarr.root(store).resolve('/data'), { kind: 'array' })

  await withPool(2, async (pool) => {
    const first = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(first.data), Array.from(values))
    const chunkFetches = fetched.filter((key) => key.includes('/c/')).length

    const second = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(second.data), Array.from(values))
    assert.equal(
      fetched.filter((key) => key.includes('/c/')).length,
      chunkFetches,
      'the second read is served from the byte cache',
    )
  })
})
