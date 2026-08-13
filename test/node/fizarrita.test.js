/**
 * getWorker/setWorker against real zarrita arrays, in plain Node.
 *
 * This is the path that used to throw `Worker is not defined` the moment a task
 * needed a worker. Runs against the built `dist/` of both packages, which is
 * what npm consumers get.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import * as zarr from 'zarrita'

import { WorkerPool } from '../../dist/index.js'
import {
  createDefaultWorker,
  DEFAULT_WORKER_URL,
  getWorker,
  setWorker,
} from '../../fizarrita/dist/index.js'

/** The bundled Node codec worker, addressed explicitly via the `workerUrl` option. */
const NODE_CODEC_WORKER = new URL(
  '../../fizarrita/dist/codec-worker-node.js',
  import.meta.url,
)

/** zstd and blosc are the compressors zarrita ships working implementations for. */
const ZSTD = [{ name: 'zstd', configuration: { level: 3 } }]
const BLOSC = [
  {
    name: 'blosc',
    configuration: {
      cname: 'lz4',
      clevel: 5,
      shuffle: 'shuffle',
      typesize: 4,
      blocksize: 0,
    },
  },
]

async function makeArray({ shape, chunk_shape, data_type = 'int32', codecs }) {
  const store = new Map()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape,
    chunk_shape,
    data_type,
    ...(codecs ? { codecs } : {}),
  })
  return arr
}

/** Run `fn` with a pool, always tearing the workers down (they hold the event loop open). */
async function withPool(size, fn) {
  const pool = new WorkerPool(size)
  try {
    return await fn(pool)
  } finally {
    pool.terminateWorkers()
  }
}

/** A Map store that records every `get`, so tests can count store round-trips. */
class CountingStore extends Map {
  reads = []
  get(key) {
    this.reads?.push(key)
    return super.get(key)
  }
}

/** An 8x8 int32 array over 4x4 chunks on a CountingStore, fully populated. */
async function makeCountedArray() {
  const store = new CountingStore()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [8, 8],
    chunk_shape: [4, 4],
    data_type: 'int32',
  })
  await zarr.set(arr, null, {
    data: Int32Array.from({ length: 64 }, (_, i) => i),
    shape: [8, 8],
    stride: [8, 1],
  })
  return { store, arr }
}

test('setWorker/getWorker round-trip a scalar fill', async () => {
  const arr = await makeArray({ shape: [8, 8], chunk_shape: [4, 4] })

  await withPool(2, async (pool) => {
    await setWorker(arr, null, 42, { pool })
    const result = await getWorker(arr, null, { pool })

    assert.deepEqual(result.shape, [8, 8])
    assert.equal(result.data.length, 64)
    assert.ok(
      result.data.every((v) => v === 42),
      'every element should be 42',
    )
  })
})

test('setWorker/getWorker round-trip full data through zstd', async () => {
  const arr = await makeArray({
    shape: [8, 8],
    chunk_shape: [4, 4],
    codecs: ZSTD,
  })
  const data = {
    data: Int32Array.from({ length: 64 }, (_, i) => i),
    shape: [8, 8],
    stride: [8, 1],
  }

  await withPool(3, async (pool) => {
    await setWorker(arr, null, data, { pool })
    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(Array.from(result.data), Array.from(data.data))
  })
})

test('results match zarrita’s own get/set', async () => {
  const arr = await makeArray({ shape: [6, 10], chunk_shape: [4, 4] })
  const reference = await makeArray({ shape: [6, 10], chunk_shape: [4, 4] })
  const data = {
    data: Int32Array.from({ length: 60 }, (_, i) => i * 3),
    shape: [6, 10],
    stride: [10, 1],
  }

  await zarr.set(reference, null, data)
  const expected = await zarr.get(reference, null)

  await withPool(2, async (pool) => {
    await setWorker(arr, null, data, { pool })
    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(result.shape, expected.shape)
    assert.deepEqual(Array.from(result.data), Array.from(expected.data))
  })
})

test('partial selections read back correctly', async () => {
  const arr = await makeArray({ shape: [8, 8], chunk_shape: [4, 4] })
  const data = {
    data: Int32Array.from({ length: 64 }, (_, i) => i),
    shape: [8, 8],
    stride: [8, 1],
  }

  await withPool(2, async (pool) => {
    await setWorker(arr, null, data, { pool })

    const row = await getWorker(arr, [1, null], { pool })
    assert.deepEqual(Array.from(row.data), [8, 9, 10, 11, 12, 13, 14, 15])

    const col = await getWorker(arr, [null, 2], { pool })
    assert.deepEqual(Array.from(col.data), [2, 10, 18, 26, 34, 42, 50, 58])

    const block = await getWorker(arr, [zarr.slice(2, 5), zarr.slice(1, 4)], { pool })
    assert.deepEqual(block.shape, [3, 3])
    assert.deepEqual(Array.from(block.data), [17, 18, 19, 25, 26, 27, 33, 34, 35])

    const scalar = await getWorker(arr, [3, 6], { pool })
    assert.equal(scalar, 30)
  })
})

test('edge chunks read back at their true extent', async () => {
  // 5x7 over 4x4 chunks: the last row and column of chunks are partial.
  const arr = await makeArray({ shape: [5, 7], chunk_shape: [4, 4] })
  const data = {
    data: Int32Array.from({ length: 35 }, (_, i) => i + 1),
    shape: [5, 7],
    stride: [7, 1],
  }

  await withPool(2, async (pool) => {
    await setWorker(arr, null, data, { pool })
    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(result.shape, [5, 7])
    assert.deepEqual(Array.from(result.data), Array.from(data.data))
  })
})

test('SharedArrayBuffer path decodes straight into shared output', async () => {
  assert.notEqual(typeof SharedArrayBuffer, 'undefined')

  const arr = await makeArray({ shape: [8, 8], chunk_shape: [4, 4], codecs: ZSTD })
  const data = {
    data: Int32Array.from({ length: 64 }, (_, i) => i * 2),
    shape: [8, 8],
    stride: [8, 1],
  }

  await withPool(2, async (pool) => {
    await setWorker(arr, null, data, { pool, useSharedArrayBuffer: true })
    const result = await getWorker(arr, null, { pool, useSharedArrayBuffer: true })

    assert.ok(
      result.data.buffer instanceof SharedArrayBuffer,
      'output should be backed by a SharedArrayBuffer',
    )
    assert.deepEqual(Array.from(result.data), Array.from(data.data))
  })
})

test('a chunk cache serves repeat reads without re-decoding', async () => {
  const arr = await makeArray({ shape: [8, 8], chunk_shape: [4, 4], codecs: BLOSC })
  const data = {
    data: Int32Array.from({ length: 64 }, (_, i) => i),
    shape: [8, 8],
    stride: [8, 1],
  }

  await withPool(2, async (pool) => {
    await setWorker(arr, null, data, { pool })

    const cache = new Map()
    const first = await getWorker(arr, null, { pool, cache })
    assert.equal(cache.size, 4, 'one entry per chunk')

    const second = await getWorker(arr, null, { pool, cache })
    assert.deepEqual(Array.from(second.data), Array.from(first.data))
    assert.deepEqual(Array.from(second.data), Array.from(data.data))
  })
})

test('a missing chunk comes back as the fill value', async () => {
  const store = new Map()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [8, 8],
    chunk_shape: [4, 4],
    data_type: 'int32',
    fill_value: -1,
  })

  await withPool(2, async (pool) => {
    // Nothing was ever written, so every chunk is absent from the store.
    const result = await getWorker(arr, null, { pool })
    assert.ok(
      result.data.every((v) => v === -1),
      'every element should be the fill value',
    )
  })
})

test('an explicit workerUrl is honoured, as a URL and as a string', async () => {
  for (const workerUrl of [NODE_CODEC_WORKER, NODE_CODEC_WORKER.href]) {
    const arr = await makeArray({ shape: [8, 8], chunk_shape: [4, 4], codecs: ZSTD })
    const data = {
      data: Int32Array.from({ length: 64 }, (_, i) => i + 1),
      shape: [8, 8],
      stride: [8, 1],
    }

    await withPool(2, async (pool) => {
      await setWorker(arr, null, data, { pool, workerUrl })
      const result = await getWorker(arr, null, { pool, workerUrl })
      assert.deepEqual(
        Array.from(result.data),
        Array.from(data.data),
        `workerUrl as ${typeof workerUrl}`,
      )
    })
  }
})

test('createDefaultWorker builds a usable worker outside get/setWorker', async () => {
  const worker = createDefaultWorker()
  try {
    const reply = await new Promise((resolve, reject) => {
      worker.addEventListener('message', resolve)
      worker.addEventListener('error', (e) => reject(new Error(e.message)))
      worker.postMessage({
        type: 'init',
        id: 1,
        metaId: 4242,
        meta: {
          data_type: 'int32',
          chunk_shape: [2, 2],
          codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
        },
      })
    })
    assert.equal(reply.data.type, 'init_ok')
    assert.equal(reply.data.id, 1)
  } finally {
    worker.terminate()
  }
})

test('DEFAULT_WORKER_URL still points at the browser entry', () => {
  // Deprecated, but exported — it must keep resolving next to the package.
  assert.ok(DEFAULT_WORKER_URL.href.endsWith('/codec-worker.js'))
})

/** Poll until `predicate` holds, failing the test after ~2s rather than hanging. */
async function waitFor(predicate, message) {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  assert.fail(message)
}

/**
 * A Map-backed store whose `get` honours `{ signal }` the way fetch does: a
 * fired signal rejects the read with the signal's reason. Keys matching `hold`
 * are parked until `releaseHeld()`, so a test controls when the signal fires
 * relative to in-flight fetches.
 */
class AbortableStore {
  constructor() {
    this.map = new Map()
    this.gets = []
    this.hold = null
    this.parked = []
  }

  async get(key, opts) {
    this.gets.push(key)
    const signal = opts?.signal
    signal?.throwIfAborted()
    if (this.hold?.(key)) {
      await new Promise((resolve, reject) => {
        this.parked.push(resolve)
        signal?.addEventListener(
          'abort',
          () => {
            // An aborted read is no longer parked — heldCount tracks live ones.
            const index = this.parked.indexOf(resolve)
            if (index !== -1) this.parked.splice(index, 1)
            reject(signal.reason)
          },
          { once: true },
        )
      })
    }
    return this.map.get(key)
  }

  async set(key, value) {
    this.map.set(key, value)
  }

  /** Stop holding — current parked reads resolve, future reads pass through. */
  releaseHeld() {
    this.hold = null
    for (const release of this.parked.splice(0)) release()
  }

  get heldCount() {
    return this.parked.length
  }

  get chunkGets() {
    return this.gets.filter((key) => key.startsWith('/data/c/'))
  }
}

async function makeAbortableArray() {
  const store = new AbortableStore()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [8, 8],
    chunk_shape: [4, 4],
    data_type: 'int32',
  })
  const data = {
    data: Int32Array.from({ length: 64 }, (_, i) => i),
    shape: [8, 8],
    stride: [8, 1],
  }
  await zarr.set(arr, null, data)
  store.gets = []
  return { store, arr, data }
}

test('an already-aborted signal rejects getWorker before any store read', async () => {
  const { store, arr } = await makeAbortableArray()

  await withPool(1, async (pool) => {
    const controller = new AbortController()
    controller.abort(new Error('stale tile'))
    await assert.rejects(
      getWorker(arr, null, { pool, signal: controller.signal }),
      /stale tile/,
    )
  })

  assert.deepEqual(store.gets, [], 'the store was never touched')
})

test('aborting mid-read cancels the in-flight fetch and drops the queued ones', async () => {
  const { store, arr } = await makeAbortableArray()
  // Park every chunk fetch except the first chunk, which the shape probe and
  // the first task read — the abort should land while a later chunk fetch is
  // in flight and two more are still queued on the pool.
  store.hold = (key) => key.startsWith('/data/c/') && key !== '/data/c/0/0'

  await withPool(1, async (pool) => {
    const controller = new AbortController()
    const read = getWorker(arr, null, { pool, signal: controller.signal })

    await waitFor(() => store.heldCount === 1, 'a chunk fetch should be parked')
    controller.abort(new Error('panned away'))
    await assert.rejects(read, /panned away/)
  })

  // Let any stray dispatch surface before asserting there was none.
  await new Promise((resolve) => setTimeout(resolve, 50))
  // Shape probe + first-chunk task + the one parked fetch: the two chunks
  // still queued when the signal fired were never fetched.
  assert.equal(store.chunkGets.length, 3, `chunk reads: ${store.chunkGets}`)
})

// A signal supplied only at the store level (inside `opts.opts`) gets the
// same treatment as `opts.signal`: the pool runs under the combined signal,
// so queued tasks are dropped rather than dispatched into failing fetches.
test('a store-level signal alone also drops the queued tasks', async () => {
  const { store, arr } = await makeAbortableArray()
  store.hold = (key) => key.startsWith('/data/c/') && key !== '/data/c/0/0'

  await withPool(1, async (pool) => {
    const controller = new AbortController()
    const read = getWorker(arr, null, {
      pool,
      opts: { signal: controller.signal },
    })

    await waitFor(() => store.heldCount === 1, 'a chunk fetch should be parked')
    controller.abort(new Error('store walked away'))
    await assert.rejects(read, /store walked away/)
  })

  // Wait for all parked fetches to be released/aborted before asserting
  // the count. This ensures no in-flight operations are still active.
  await waitFor(() => store.heldCount === 0, 'all parked fetches should be released')
  assert.equal(store.chunkGets.length, 3, `chunk reads: ${store.chunkGets}`)
})

// Concurrent reads of the same chunks share one fetch, and that fetch runs
// with its producer's signal. A read that did not abort must survive its
// producer walking away — by re-fetching the chunk itself — even when the
// abort carried a custom reason.
test('a concurrent read survives another read aborting their shared chunks', { timeout: 15_000 }, async () => {
  const { store, arr, data } = await makeAbortableArray()
  store.hold = (key) => key.startsWith('/data/c/') && key !== '/data/c/0/0'

  await withPool(2, async (poolA) => {
    await withPool(2, async (poolB) => {
      const controller = new AbortController()
      const readA = getWorker(arr, null, { pool: poolA, signal: controller.signal })

      // Both of A's parked fetches in flight — its other tasks are queued.
      await waitFor(() => store.heldCount === 2, 'read A should have two parked fetches')

      const readB = getWorker(arr, null, { pool: poolB })
      // B re-reads the unparked first chunk itself (A's share of it has long
      // settled), then joins A's parked in-flight fetches as a follower. Three
      // reads of that chunk by then: A's shape probe, A's fetch, B's fetch —
      // B does not probe, the array info is memoised from A's resolution.
      await waitFor(
        () => store.chunkGets.filter((k) => k === '/data/c/0/0').length >= 3,
        'read B should have read the first chunk',
      )
      // Wait for B to have progressed into fetching additional chunks. Since
      // B joining A's in-flight fetches as a follower doesn't touch the store,
      // we wait for either (a) B creating independent fetches (heldCount > 2),
      // or (b) the held state to stabilize, indicating B has had the
      // opportunity to join. We verify this by checking the held count remains
      // at 2 (A's two parked fetches) across multiple poll iterations.
      let stableCount = 0
      await waitFor(() => {
        const current = store.heldCount
        if (current > 2) return true // B made independent fetches
        if (current === 2) stableCount++
        else stableCount = 0
        return stableCount >= 3 // Stable for 3 iterations (30ms of polls)
      }, 'read B should have attempted to join or created independent fetches')

      controller.abort(new Error('viewport moved'))
      await assert.rejects(readA, /viewport moved/)

      // B's shared chunks died with A's signal; B must now be re-fetching
      // them under its own steam. Un-park everything and let it finish.
      await waitFor(() => store.heldCount >= 1, 'read B should retry the aborted chunks')
      store.releaseHeld()

      const result = await readB
      assert.deepEqual(Array.from(result.data), Array.from(data.data))
    })
  })
})

test('a decode failure rejects instead of hanging', { timeout: 15_000 }, async () => {
  const store = new Map()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [4, 4],
    chunk_shape: [4, 4],
    data_type: 'int32',
    codecs: ZSTD,
  })
  // Not a zstd frame — the worker's decode will throw.
  store.set('/data/c/0/0', new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]))

  await withPool(1, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }))
  })
})

// Issue #6 — the metadata read and chunk-shape probe used to run on every
// getWorker call, ahead of the chunk cache, so a fully populated cache could
// never eliminate them. Both are now memoised per (store, array path).

test('a warm chunk cache serves a repeat read with zero store round-trips', async () => {
  const { store, arr } = await makeCountedArray()

  await withPool(2, async (pool) => {
    const cache = new Map()
    store.reads.length = 0
    const first = await getWorker(arr, null, { pool, cache })
    assert.ok(
      store.reads.includes('/data/zarr.json'),
      'the first read resolves metadata from the store',
    )

    store.reads.length = 0
    const second = await getWorker(arr, null, { pool, cache })
    assert.deepEqual(Array.from(second.data), Array.from(first.data))
    assert.deepEqual(store.reads, [], 'the repeat read never touches the store')
  })
})

test('repeat reads without a cache pay only the chunk fetches', async () => {
  const { store, arr } = await makeCountedArray()

  await withPool(2, async (pool) => {
    await getWorker(arr, null, { pool })

    store.reads.length = 0
    await getWorker(arr, null, { pool })
    assert.deepEqual(
      [...store.reads].sort(),
      ['/data/c/0/0', '/data/c/0/1', '/data/c/1/0', '/data/c/1/1'],
      'no metadata read, no probe — one fetch per chunk',
    )
  })
})

test('concurrent reads on a cold array share one metadata read and one probe', async () => {
  const { store, arr } = await makeCountedArray()

  await withPool(2, async (pool) => {
    const cache = new Map()
    store.reads.length = 0
    const [a, b] = await Promise.all([
      getWorker(arr, null, { pool, cache }),
      getWorker(arr, null, { pool, cache }),
    ])
    assert.deepEqual(Array.from(a.data), Array.from(b.data))

    const metadataReads = store.reads.filter((k) => k === '/data/zarr.json')
    assert.equal(metadataReads.length, 1, 'one zarr.json read for both calls')
    // 1 metadata read + 1 probe of c/0/0 + 4 chunk fetches: the probe and the
    // chunk fetch of c/0/0 both count, everything else exactly once.
    assert.equal(store.reads.length, 6, `reads: ${store.reads.join(', ')}`)
  })
})

test('store options reach the metadata reads, not just the probe', async () => {
  const store = new CountingStore()
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [8, 8],
    chunk_shape: [4, 4],
    data_type: 'int32',
  })
  await zarr.set(arr, null, {
    data: Int32Array.from({ length: 64 }, (_, i) => i),
    shape: [8, 8],
    stride: [8, 1],
  })

  // Record the options every read is given, keyed by path.
  const seenOpts = new Map()
  const realGet = CountingStore.prototype.get.bind(store)
  store.get = (key, opts) => {
    seenOpts.set(key, opts)
    return realGet(key)
  }

  const marker = { headers: { authorization: 'sentinel' } }
  await withPool(2, async (pool) => {
    await getWorker(arr, null, { pool, opts: marker })
  })

  // The metadata read used to be the one store request that silently dropped
  // the caller's options while the probe and chunk fetches honoured them.
  assert.equal(seenOpts.get('/data/zarr.json'), marker)
  assert.equal(seenOpts.get('/data/c/0/0'), marker)
  assert.equal(seenOpts.get('/data/c/1/1'), marker)
})

test('a probe that fails transiently is retried, not memoised as a missed correction', async () => {
  const store = new CountingStore()
  // Chunks are really 4x8. The metadata is rewritten below to claim 4x4, so
  // the shape probe has real work to do — exactly the case where silently
  // memoising "no correction needed" would corrupt every later read.
  const arr = await zarr.create(zarr.root(store).resolve('/data'), {
    shape: [8, 8],
    chunk_shape: [4, 8],
    data_type: 'int32',
  })
  const expected = Int32Array.from({ length: 64 }, (_, i) => i)
  await zarr.set(arr, null, { data: expected, shape: [8, 8], stride: [8, 1] })

  const meta = JSON.parse(new TextDecoder().decode(store.get('/data/zarr.json')))
  meta.chunk_grid.configuration.chunk_shape = [4, 4]
  store.set(
    '/data/zarr.json',
    new TextEncoder().encode(JSON.stringify(meta)),
  )
  const misdeclared = await zarr.open(zarr.root(store).resolve('/data'), {
    kind: 'array',
  })

  // Fail the probe's chunk fetch exactly once. The metadata read must still
  // succeed, or the resolution would reject and be evicted by the other path.
  let probeFailures = 1
  const realGet = CountingStore.prototype.get.bind(store)
  store.get = (key) => {
    if (key.includes('/c/') && probeFailures > 0) {
      probeFailures--
      throw new Error('transient probe failure')
    }
    return realGet(key)
  }

  await withPool(2, async (pool) => {
    // The first read loses the probe and falls back to the declared 4x4.
    // Whether it then throws or returns misshapen data is not the point —
    // what matters is that the miss is not remembered.
    await getWorker(misdeclared, null, { pool }).catch(() => {})

    // The second read probes again and finds the real 4x8 chunking.
    const result = await getWorker(misdeclared, null, { pool })
    assert.deepEqual(result.shape, [8, 8])
    assert.deepEqual(Array.from(result.data), Array.from(expected))
  })
})

test('a failed metadata read is retried, not memoised', async () => {
  const { store, arr } = await makeCountedArray()

  let failures = 1
  const realGet = CountingStore.prototype.get.bind(store)
  store.get = (key) => {
    if (failures > 0) {
      failures--
      throw new Error('transient store failure')
    }
    return realGet(key)
  }

  await withPool(2, async (pool) => {
    await assert.rejects(getWorker(arr, null, { pool }), /transient/)

    const result = await getWorker(arr, null, { pool })
    assert.deepEqual(result.shape, [8, 8])
    assert.equal(result.data[63], 63)
  })
})
