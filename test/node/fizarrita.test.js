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
      // settled), then joins A's parked in-flight fetches as a follower.
      await waitFor(
        () => store.chunkGets.filter((k) => k === '/data/c/0/0').length >= 4,
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
