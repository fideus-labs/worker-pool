/**
 * Integration tests for @fideus-labs/zarrita.js getWorker/setWorker.
 *
 * These tests exercise the real zarrita library with actual codec pipelines,
 * verifying that getWorker and setWorker produce identical results to
 * zarrita's built-in get/set but with codec work offloaded to Web Workers.
 */

import { test, expect, type Page } from '@playwright/test'

// Helper: evaluate code in the browser context and return the result
async function evaluate<T>(page: Page, fn: string): Promise<T> {
  return page.evaluate(fn) as Promise<T>
}

test.describe('@fideus-labs/zarrita.js — getWorker / setWorker', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for all modules to load
    await page.waitForFunction(() => {
      return (
        typeof window.zarr !== 'undefined' &&
        typeof window.getWorker === 'function' &&
        typeof window.setWorker === 'function' &&
        typeof window.WorkerPool !== 'undefined'
      )
    })
  })

  // -------------------------------------------------------------------------
  // Basic round-trip: set then get with BytesCodec (no compression)
  // -------------------------------------------------------------------------

  test('getWorker reads data written by zarr.set (BytesCodec)', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      // Create an in-memory zarr v3 array
      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'int32',
      })

      // Write data using zarrita's built-in set
      const data = new Int32Array([
        1, 2, 3, 4,
        5, 6, 7, 8,
        9, 10, 11, 12,
        13, 14, 15, 16,
      ])
      await zarr.set(arr, null, {
        data,
        shape: [4, 4],
        stride: [4, 1],
      })

      // Read using getWorker
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
      }
    })

    expect(result.data).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    expect(result.shape).toEqual([4, 4])
  })

  // -------------------------------------------------------------------------
  // setWorker + getWorker round-trip with BytesCodec
  // -------------------------------------------------------------------------

  test('setWorker + getWorker round-trip (BytesCodec)', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6],
        chunk_shape: [3],
        data_type: 'float32',
      })

      // Write using setWorker
      const data = new Float32Array([10, 20, 30, 40, 50, 60])
      await setWorker(arr, null, {
        data,
        shape: [6],
        stride: [1],
      }, { pool })

      // Read using getWorker
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Float32Array)
    })

    expect(result).toEqual([10, 20, 30, 40, 50, 60])
  })

  // -------------------------------------------------------------------------
  // setWorker with scalar value
  // -------------------------------------------------------------------------

  test('setWorker with scalar value', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [3, 3],
        chunk_shape: [3, 3],
        data_type: 'float64',
      })

      // Set all elements to 42.0
      await setWorker(arr, null, 42.0, { pool })

      // Read back
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Float64Array)
    })

    expect(result).toEqual([42, 42, 42, 42, 42, 42, 42, 42, 42])
  })

  // -------------------------------------------------------------------------
  // Slice selection with getWorker
  // -------------------------------------------------------------------------

  test('getWorker with slice selection', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [10],
        chunk_shape: [5],
        data_type: 'int32',
      })

      // Write sequential data
      const data = new Int32Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      await zarr.set(arr, null, { data, shape: [10], stride: [1] })

      // Read a slice [2:7]
      const chunk = await getWorker(arr, [zarr.slice(2, 7)], { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
      }
    })

    expect(result.data).toEqual([2, 3, 4, 5, 6])
    expect(result.shape).toEqual([5])
  })

  // -------------------------------------------------------------------------
  // Partial chunk update with setWorker
  // -------------------------------------------------------------------------

  test('setWorker partial chunk update', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6],
        chunk_shape: [3],
        data_type: 'int32',
      })

      // Fill with 1s
      await setWorker(arr, null, 1, { pool })

      // Partially update: set [1:4] = 99
      await setWorker(arr, [zarr.slice(1, 4)], 99, { pool })

      // Read full array
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Int32Array)
    })

    // [1, 99, 99, 99, 1, 1]
    expect(result).toEqual([1, 99, 99, 99, 1, 1])
  })

  // -------------------------------------------------------------------------
  // Comparison: getWorker vs zarr.get produce same results
  // -------------------------------------------------------------------------

  test('getWorker matches zarr.get results', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [8, 8],
        chunk_shape: [4, 4],
        data_type: 'float32',
      })

      // Write data
      const data = new Float32Array(64)
      for (let i = 0; i < 64; i++) data[i] = i * 1.5
      await zarr.set(arr, null, { data, shape: [8, 8], stride: [8, 1] })

      // Read with zarr.get
      const builtinResult = await zarr.get(arr, null)

      // Read with getWorker
      const workerResult = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return {
        builtinData: Array.from(builtinResult.data as Float32Array),
        workerData: Array.from(workerResult.data as Float32Array),
        builtinShape: builtinResult.shape,
        workerShape: workerResult.shape,
      }
    })

    expect(result.workerData).toEqual(result.builtinData)
    expect(result.workerShape).toEqual(result.builtinShape)
  })

  // -------------------------------------------------------------------------
  // setWorker + zarr.get (cross-compatibility)
  // -------------------------------------------------------------------------

  test('setWorker data readable by zarr.get', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4],
        chunk_shape: [2],
        data_type: 'uint8',
      })

      // Write with setWorker
      await setWorker(arr, null, {
        data: new Uint8Array([10, 20, 30, 40]),
        shape: [4],
        stride: [1],
      }, { pool })
      pool.terminateWorkers()

      // Read with zarr.get (built-in)
      const chunk = await zarr.get(arr, null)
      return Array.from(chunk.data as Uint8Array)
    })

    expect(result).toEqual([10, 20, 30, 40])
  })

  // -------------------------------------------------------------------------
  // Worker recycling
  // -------------------------------------------------------------------------

  test('workers are recycled across getWorker calls', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4],
        chunk_shape: [2],
        data_type: 'int32',
      })

      await zarr.set(arr, null, {
        data: new Int32Array([1, 2, 3, 4]),
        shape: [4],
        stride: [1],
      })

      // First call — workers are created
      await getWorker(arr, null, { pool })
      const workersAfterFirst = pool.workerQueue.filter(w => w !== null).length

      // Second call — workers should be recycled
      await getWorker(arr, null, { pool })
      const workersAfterSecond = pool.workerQueue.filter(w => w !== null).length

      pool.terminateWorkers()

      return { workersAfterFirst, workersAfterSecond }
    })

    // Workers created in first call should still exist in second
    expect(result.workersAfterFirst).toBeGreaterThan(0)
    expect(result.workersAfterSecond).toBeGreaterThanOrEqual(result.workersAfterFirst)
  })

  // -------------------------------------------------------------------------
  // Multiple data types
  // -------------------------------------------------------------------------

  test('supports float64 data type', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [3],
        chunk_shape: [3],
        data_type: 'float64',
      })

      await setWorker(arr, null, {
        data: new Float64Array([1.111111111111, 2.222222222222, 3.333333333333]),
        shape: [3],
        stride: [1],
      }, { pool })

      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Float64Array)
    })

    expect(result[0]).toBeCloseTo(1.111111111111, 10)
    expect(result[1]).toBeCloseTo(2.222222222222, 10)
    expect(result[2]).toBeCloseTo(3.333333333333, 10)
  })

  test('supports uint16 data type', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4],
        chunk_shape: [2],
        data_type: 'uint16',
      })

      await setWorker(arr, null, {
        data: new Uint16Array([100, 200, 300, 65535]),
        shape: [4],
        stride: [1],
      }, { pool })

      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Uint16Array)
    })

    expect(result).toEqual([100, 200, 300, 65535])
  })

  // -------------------------------------------------------------------------
  // Concurrency control
  // -------------------------------------------------------------------------

  test('respects concurrency option', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(4)

      const store = zarr.root()
      // 16 chunks to process
      const arr = await zarr.create(store, {
        shape: [16],
        chunk_shape: [1],
        data_type: 'int32',
      })

      const data = new Int32Array(16)
      for (let i = 0; i < 16; i++) data[i] = i * 10
      await zarr.set(arr, null, { data, shape: [16], stride: [1] })

      // Read with concurrency limited to 2
      const chunk = await getWorker(arr, null, { pool, concurrency: 2 })
      pool.terminateWorkers()

      return Array.from(chunk.data as Int32Array)
    })

    const expected = Array.from({ length: 16 }, (_, i) => i * 10)
    expect(result).toEqual(expected)
  })

  // -------------------------------------------------------------------------
  // Multi-dimensional array with multiple chunks
  // -------------------------------------------------------------------------

  test('handles 3D array with multiple chunks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(4)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4, 4, 4],
        chunk_shape: [2, 2, 2],
        data_type: 'float32',
      })

      // Fill with sequential values
      const data = new Float32Array(64)
      for (let i = 0; i < 64; i++) data[i] = i
      await zarr.set(arr, null, { data, shape: [4, 4, 4], stride: [16, 4, 1] })

      // Read full array via workers
      const workerChunk = await getWorker(arr, null, { pool })

      // Read via built-in
      const builtinChunk = await zarr.get(arr, null)

      pool.terminateWorkers()

      return {
        match: Array.from(workerChunk.data as Float32Array).every(
          (v, i) => v === (builtinChunk.data as Float32Array)[i]
        ),
        shape: workerChunk.shape,
        length: (workerChunk.data as Float32Array).length,
      }
    })

    expect(result.match).toBe(true)
    expect(result.shape).toEqual([4, 4, 4])
    expect(result.length).toBe(64)
  })

  // -------------------------------------------------------------------------
  // Integer indexing (scalar result)
  // -------------------------------------------------------------------------

  test('getWorker returns scalar for integer selection', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [5],
        chunk_shape: [5],
        data_type: 'int32',
      })

      await zarr.set(arr, null, {
        data: new Int32Array([10, 20, 30, 40, 50]),
        shape: [5],
        stride: [1],
      })

      // Select a single element
      const scalar = await getWorker(arr, [3], { pool })
      pool.terminateWorkers()

      return scalar
    })

    expect(result).toBe(40)
  })

  // -------------------------------------------------------------------------
  // Fill value handling
  // -------------------------------------------------------------------------

  test('getWorker handles fill value for missing chunks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6],
        chunk_shape: [3],
        data_type: 'int32',
        fill_value: -1,
      })

      // Only write to the first chunk
      await zarr.set(arr, [zarr.slice(0, 3)], {
        data: new Int32Array([10, 20, 30]),
        shape: [3],
        stride: [1],
      })

      // Read full array — second chunk should be fill value
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Int32Array)
    })

    expect(result).toEqual([10, 20, 30, -1, -1, -1])
  })

  // -------------------------------------------------------------------------
  // GZip codec (real compression)
  // -------------------------------------------------------------------------
  // NOTE: zarrita's built-in gzip codec only supports DECODING (via
  // DecompressionStream). Encoding requires `numcodecs/gzip` to be registered.
  // We test gzip decoding by manually gzip-compressing data and writing the
  // raw bytes directly to the store, then using getWorker to decode.

  test('getWorker decodes gzip-compressed chunks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4],
        chunk_shape: [4],
        data_type: 'int32',
        codecs: [
          { name: 'bytes', configuration: { endian: 'little' } },
          { name: 'gzip', configuration: { level: 1 } },
        ],
      })

      // Manually gzip-compress and write raw chunk bytes
      const data = new Int32Array([100, 200, 300, 400])
      const rawBytes = new Uint8Array(data.buffer)

      // Use CompressionStream to gzip the bytes
      const compressedStream = new Response(rawBytes).body!
        .pipeThrough(new CompressionStream('gzip'))
      const compressedBytes = new Uint8Array(
        await new Response(compressedStream).arrayBuffer()
      )

      // Write compressed bytes directly to store at the chunk key
      const mapStore = arr.store as unknown as { get(k: string): Uint8Array | undefined; set(k: string, v: Uint8Array): void }
      mapStore.set('/c/0', compressedBytes)

      // Read using getWorker (which should gzip-decode in worker)
      const chunk = await getWorker(arr, null, { pool })

      // Also read using zarr.get for comparison
      const builtinChunk = await zarr.get(arr, null)

      pool.terminateWorkers()

      return {
        workerData: Array.from(chunk.data as Int32Array),
        builtinData: Array.from(builtinChunk.data as Int32Array),
      }
    })

    expect(result.workerData).toEqual([100, 200, 300, 400])
    expect(result.workerData).toEqual(result.builtinData)
  })

})
