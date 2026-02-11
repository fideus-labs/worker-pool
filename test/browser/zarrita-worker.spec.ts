/**
 * Integration tests for @fideus-labs/fizarrita getWorker/setWorker.
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

test.describe('@fideus-labs/fizarrita — getWorker / setWorker', () => {

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

  test('pool size controls concurrency', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      // Pool size IS the concurrency (no separate concurrency option)
      const pool = new WorkerPool(2)

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

      // Read — pool size (2) controls concurrency
      const chunk = await getWorker(arr, null, { pool })
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

  // -------------------------------------------------------------------------
  // SharedArrayBuffer support
  // -------------------------------------------------------------------------

  test('getWorker with useSharedArrayBuffer returns SharedArrayBuffer-backed output', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'int32',
      })

      // Write data
      const data = new Int32Array(16)
      for (let i = 0; i < 16; i++) data[i] = i + 1
      await zarr.set(arr, null, { data, shape: [4, 4], stride: [4, 1] })

      // Read with SharedArrayBuffer
      const chunk = await getWorker(arr, null, { pool, useSharedArrayBuffer: true })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
        isShared: chunk.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.isShared).toBe(true)
    expect(result.shape).toEqual([4, 4])
    expect(result.data).toEqual(Array.from({ length: 16 }, (_, i) => i + 1))
  })

  test('getWorker without useSharedArrayBuffer returns regular ArrayBuffer', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4],
        chunk_shape: [4],
        data_type: 'int32',
      })

      await zarr.set(arr, null, {
        data: new Int32Array([1, 2, 3, 4]),
        shape: [4],
        stride: [1],
      })

      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        isShared: chunk.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.isShared).toBe(false)
    expect(result.data).toEqual([1, 2, 3, 4])
  })

  test('getWorker with useSharedArrayBuffer works with slicing', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [10],
        chunk_shape: [5],
        data_type: 'int32',
      })

      const data = new Int32Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
      await zarr.set(arr, null, { data, shape: [10], stride: [1] })

      // Read a slice [2:7] with SAB
      const chunk = await getWorker(arr, [zarr.slice(2, 7)], {
        pool,
        useSharedArrayBuffer: true,
      })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
        isShared: chunk.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.isShared).toBe(true)
    expect(result.shape).toEqual([5])
    expect(result.data).toEqual([2, 3, 4, 5, 6])
  })

  test('getWorker with useSharedArrayBuffer handles fill-value chunks', async ({ page }) => {
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
      const chunk = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        isShared: chunk.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.isShared).toBe(true)
    expect(result.data).toEqual([10, 20, 30, -1, -1, -1])
  })

  test('getWorker with useSharedArrayBuffer matches non-SAB results', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [8, 8],
        chunk_shape: [4, 4],
        data_type: 'float32',
      })

      const data = new Float32Array(64)
      for (let i = 0; i < 64; i++) data[i] = i * 1.5
      await zarr.set(arr, null, { data, shape: [8, 8], stride: [8, 1] })

      // Read without SAB
      const normalResult = await getWorker(arr, null, { pool })

      // Read with SAB
      const sabResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })
      pool.terminateWorkers()

      return {
        normalData: Array.from(normalResult.data as Float32Array),
        sabData: Array.from(sabResult.data as Float32Array),
        normalIsShared: normalResult.data.buffer instanceof SharedArrayBuffer,
        sabIsShared: sabResult.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.normalIsShared).toBe(false)
    expect(result.sabIsShared).toBe(true)
    expect(result.sabData).toEqual(result.normalData)
  })

  test('getWorker with useSharedArrayBuffer: 3D multi-chunk array', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
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

      // Read with SAB — 8 chunks decoded by workers into shared memory
      const sabChunk = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })

      // Read with built-in zarr.get for comparison
      const builtinChunk = await zarr.get(arr, null)

      pool.terminateWorkers()

      return {
        match: Array.from(sabChunk.data as Float32Array).every(
          (v, i) => v === (builtinChunk.data as Float32Array)[i]
        ),
        shape: sabChunk.shape,
        length: (sabChunk.data as Float32Array).length,
        isShared: sabChunk.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.match).toBe(true)
    expect(result.shape).toEqual([4, 4, 4])
    expect(result.length).toBe(64)
    expect(result.isShared).toBe(true)
  })

  test('getWorker with useSharedArrayBuffer: gzip-compressed chunks', async ({ page }) => {
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
      const compressedStream = new Response(rawBytes).body!
        .pipeThrough(new CompressionStream('gzip'))
      const compressedBytes = new Uint8Array(
        await new Response(compressedStream).arrayBuffer()
      )

      const mapStore = arr.store as unknown as { set(k: string, v: Uint8Array): void }
      mapStore.set('/c/0', compressedBytes)

      // Read using getWorker with SAB
      const chunk = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        isShared: chunk.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.data).toEqual([100, 200, 300, 400])
    expect(result.isShared).toBe(true)
  })

  // -------------------------------------------------------------------------
  // setWorker with SharedArrayBuffer support
  // -------------------------------------------------------------------------

  test('setWorker with useSharedArrayBuffer: full array write', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6],
        chunk_shape: [3],
        data_type: 'float32',
      })

      // Write using setWorker with SAB
      const data = new Float32Array([10, 20, 30, 40, 50, 60])
      await setWorker(arr, null, {
        data,
        shape: [6],
        stride: [1],
      }, { pool, useSharedArrayBuffer: true })

      // Read back using zarr.get (built-in) to verify
      const chunk = await zarr.get(arr, null)
      pool.terminateWorkers()

      return Array.from(chunk.data as Float32Array)
    })

    expect(result).toEqual([10, 20, 30, 40, 50, 60])
  })

  test('setWorker with useSharedArrayBuffer: partial chunk update', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [6],
        chunk_shape: [3],
        data_type: 'int32',
      })

      // Fill with 1s using regular setWorker
      await setWorker(arr, null, 1, { pool })

      // Partially update [1:4] = 99 using SAB path
      await setWorker(arr, [zarr.slice(1, 4)], 99, { pool, useSharedArrayBuffer: true })

      // Read full array
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Int32Array)
    })

    expect(result).toEqual([1, 99, 99, 99, 1, 1])
  })

  test('setWorker with useSharedArrayBuffer: scalar value', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [3, 3],
        chunk_shape: [3, 3],
        data_type: 'float64',
      })

      // Set all elements to 42.0 with SAB
      await setWorker(arr, null, 42.0, { pool, useSharedArrayBuffer: true })

      // Read back
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return Array.from(chunk.data as Float64Array)
    })

    expect(result).toEqual([42, 42, 42, 42, 42, 42, 42, 42, 42])
  })

  test('setWorker with useSharedArrayBuffer matches non-SAB results', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, setWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      // Write same data with and without SAB, compare
      const data = new Float32Array(16)
      for (let i = 0; i < 16; i++) data[i] = i * 2.5

      // Without SAB
      const store1 = zarr.root()
      const arr1 = await zarr.create(store1, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'float32',
      })
      await setWorker(arr1, null, {
        data: new Float32Array(data),
        shape: [4, 4],
        stride: [4, 1],
      }, { pool })

      // With SAB
      const store2 = zarr.root()
      const arr2 = await zarr.create(store2, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'float32',
      })
      await setWorker(arr2, null, {
        data: new Float32Array(data),
        shape: [4, 4],
        stride: [4, 1],
      }, { pool, useSharedArrayBuffer: true })

      // Read both back with zarr.get
      const result1 = await zarr.get(arr1, null)
      const result2 = await zarr.get(arr2, null)

      pool.terminateWorkers()

      return {
        normalData: Array.from(result1.data as Float32Array),
        sabData: Array.from(result2.data as Float32Array),
      }
    })

    expect(result.sabData).toEqual(result.normalData)
  })

  test('getWorker with useSharedArrayBuffer: multiple data types', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)
      const results: { dtype: string; data: number[]; isShared: boolean }[] = []

      for (const [dtype, TypedArr, vals] of [
        ['uint8', Uint8Array, [1, 2, 255]] as const,
        ['uint16', Uint16Array, [100, 200, 65535]] as const,
        ['float64', Float64Array, [1.1, 2.2, 3.3]] as const,
      ]) {
        const store = zarr.root()
        const arr = await zarr.create(store, {
          shape: [3],
          chunk_shape: [3],
          data_type: dtype,
        })

        await zarr.set(arr, null, {
          data: new TypedArr(vals),
          shape: [3],
          stride: [1],
        })

        const chunk = await getWorker(arr, null, {
          pool,
          useSharedArrayBuffer: true,
        })

        results.push({
          dtype,
          data: Array.from(chunk.data as unknown as ArrayLike<number>),
          isShared: chunk.data.buffer instanceof SharedArrayBuffer,
        })
      }

      pool.terminateWorkers()
      return results
    })

    for (const r of result) {
      expect(r.isShared).toBe(true)
    }
    expect(result[0].data).toEqual([1, 2, 255])
    expect(result[1].data).toEqual([100, 200, 65535])
    expect(result[2].data[0]).toBeCloseTo(1.1, 10)
    expect(result[2].data[1]).toBeCloseTo(2.2, 10)
    expect(result[2].data[2]).toBeCloseTo(3.3, 10)
  })

  // -------------------------------------------------------------------------
  // SAB decode_into bug: larger 3D arrays where chunks don't evenly divide shape
  // -------------------------------------------------------------------------

  test('SAB decode_into: 3D array with non-evenly-divisible chunks (OME-Zarr-like)', async ({ page }) => {
    // This test mirrors real OME-Zarr data: shape [109, 256, 256] with
    // chunks [28, 64, 128], uint16. Edge chunks in dim 0 have effective
    // size 25 (109 - 28*3 = 25), not 28.
    // We use a smaller but proportionally similar geometry to keep the test
    // fast while still exercising the edge-chunk code path.
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(4)

      // Shape [11, 8, 8] with chunks [3, 4, 4]:
      // dim 0: 4 chunks — 3 full (3) + 1 edge (2)
      // dim 1: 2 chunks — both full (4)
      // dim 2: 2 chunks — both full (4)
      const shape = [11, 8, 8] as const
      const chunkShape = [3, 4, 4] as const
      const totalSize = shape[0] * shape[1] * shape[2] // 704

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [...shape],
        chunk_shape: [...chunkShape],
        data_type: 'uint16',
      })

      // Fill with sequential values
      const data = new Uint16Array(totalSize)
      for (let i = 0; i < totalSize; i++) data[i] = i + 1
      await zarr.set(arr, null, {
        data,
        shape: [...shape],
        stride: [shape[1] * shape[2], shape[2], 1],
      })

      // Read with SAB
      const sabResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })

      // Read without SAB (known-good path)
      const normalResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: false,
      })

      // Also read with zarr.get for ground truth
      const builtinResult = await zarr.get(arr, null)

      pool.terminateWorkers()

      const sabData = Array.from(sabResult.data as Uint16Array)
      const normalData = Array.from(normalResult.data as Uint16Array)
      const builtinData = Array.from(builtinResult.data as Uint16Array)

      // Find first mismatch
      let firstMismatchIdx = -1
      for (let i = 0; i < totalSize; i++) {
        if (sabData[i] !== normalData[i]) {
          firstMismatchIdx = i
          break
        }
      }

      return {
        sabMatchesNormal: sabData.every((v, i) => v === normalData[i]),
        normalMatchesBuiltin: normalData.every((v, i) => v === builtinData[i]),
        sabShape: sabResult.shape,
        normalShape: normalResult.shape,
        isShared: sabResult.data.buffer instanceof SharedArrayBuffer,
        totalSize,
        firstMismatchIdx,
        // Include some data around mismatch for debugging
        sabSlice: firstMismatchIdx >= 0
          ? sabData.slice(Math.max(0, firstMismatchIdx - 2), firstMismatchIdx + 5)
          : [],
        normalSlice: firstMismatchIdx >= 0
          ? normalData.slice(Math.max(0, firstMismatchIdx - 2), firstMismatchIdx + 5)
          : [],
      }
    })

    expect(result.normalMatchesBuiltin).toBe(true)
    expect(result.isShared).toBe(true)
    expect(result.sabShape).toEqual([11, 8, 8])
    expect(result.normalShape).toEqual([11, 8, 8])
    // The key assertion: SAB path should produce identical results
    if (!result.sabMatchesNormal) {
      throw new Error(
        `SAB decode_into mismatch at index ${result.firstMismatchIdx}. ` +
        `SAB[...]: [${result.sabSlice}], Normal[...]: [${result.normalSlice}]`
      )
    }
    expect(result.sabMatchesNormal).toBe(true)
  })

  test('SAB decode_into: 3D uint16 matching real OME-Zarr geometry', async ({ page }) => {
    // Full-scale test: shape [13, 32, 32] with chunks [4, 8, 16]
    // dim 0: 4 chunks — 3 full (4) + 1 edge (1)
    // dim 1: 4 chunks — all full (8)
    // dim 2: 2 chunks — all full (16)
    // Total: 32 chunks, including edge chunks
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(4)

      const shape = [13, 32, 32] as const
      const chunkShape = [4, 8, 16] as const
      const totalSize = shape[0] * shape[1] * shape[2] // 13312

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [...shape],
        chunk_shape: [...chunkShape],
        data_type: 'uint16',
      })

      // Fill with unique values (i + 1 so no zeros)
      const data = new Uint16Array(totalSize)
      for (let i = 0; i < totalSize; i++) data[i] = (i % 65535) + 1
      await zarr.set(arr, null, {
        data,
        shape: [...shape],
        stride: [shape[1] * shape[2], shape[2], 1],
      })

      // SAB path
      const sabResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })

      // Normal path (ground truth)
      const normalResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: false,
      })

      pool.terminateWorkers()

      const sabData = Array.from(sabResult.data as Uint16Array)
      const normalData = Array.from(normalResult.data as Uint16Array)

      let mismatchCount = 0
      let firstMismatchIdx = -1
      for (let i = 0; i < totalSize; i++) {
        if (sabData[i] !== normalData[i]) {
          mismatchCount++
          if (firstMismatchIdx === -1) firstMismatchIdx = i
        }
      }

      return {
        match: mismatchCount === 0,
        mismatchCount,
        firstMismatchIdx,
        totalSize,
        isShared: sabResult.data.buffer instanceof SharedArrayBuffer,
        sabSlice: firstMismatchIdx >= 0
          ? sabData.slice(firstMismatchIdx, firstMismatchIdx + 10)
          : [],
        normalSlice: firstMismatchIdx >= 0
          ? normalData.slice(firstMismatchIdx, firstMismatchIdx + 10)
          : [],
      }
    })

    expect(result.isShared).toBe(true)
    if (!result.match) {
      throw new Error(
        `SAB decode_into: ${result.mismatchCount}/${result.totalSize} mismatches. ` +
        `First at index ${result.firstMismatchIdx}. ` +
        `SAB: [${result.sabSlice}], Normal: [${result.normalSlice}]`
      )
    }
    expect(result.match).toBe(true)
  })

  test('SAB decode_into: 2D with non-evenly-divisible chunks', async ({ page }) => {
    // Simpler 2D case: shape [7, 5] with chunks [3, 2]
    // dim 0: 3 chunks — 2 full (3) + 1 edge (1)
    // dim 1: 3 chunks — 2 full (2) + 1 edge (1)
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const shape = [7, 5] as const
      const chunkShape = [3, 2] as const
      const totalSize = shape[0] * shape[1] // 35

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [...shape],
        chunk_shape: [...chunkShape],
        data_type: 'int32',
      })

      const data = new Int32Array(totalSize)
      for (let i = 0; i < totalSize; i++) data[i] = (i + 1) * 10
      await zarr.set(arr, null, {
        data,
        shape: [...shape],
        stride: [shape[1], 1],
      })

      // SAB path
      const sabResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })

      // Normal path
      const normalResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: false,
      })

      pool.terminateWorkers()

      const sabData = Array.from(sabResult.data as Int32Array)
      const normalData = Array.from(normalResult.data as Int32Array)

      return {
        sabData,
        normalData,
        match: sabData.every((v, i) => v === normalData[i]),
        isShared: sabResult.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.isShared).toBe(true)
    expect(result.sabData).toEqual(result.normalData)
  })

  test('SAB decode_into: large 3D uint16 matching exact OME-Zarr geometry', async ({ page }) => {
    // Exact geometry from mri_woman.ome.zarr: shape [109, 256, 256] chunks [28, 64, 128]
    // This is a large test — 109*256*256 = 7143424 elements (14MB of uint16)
    test.setTimeout(60_000)

    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(4)

      const shape = [109, 256, 256] as const
      const chunkShape = [28, 64, 128] as const
      const totalSize = shape[0] * shape[1] * shape[2]

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [...shape],
        chunk_shape: [...chunkShape],
        data_type: 'uint16',
      })

      // Fill with a pattern that makes corruption obvious
      // Use row index * 1000 + col index so each element is unique
      const data = new Uint16Array(totalSize)
      for (let z = 0; z < shape[0]; z++) {
        for (let y = 0; y < shape[1]; y++) {
          for (let x = 0; x < shape[2]; x++) {
            const idx = z * shape[1] * shape[2] + y * shape[2] + x
            data[idx] = ((z * 7 + y * 3 + x) % 65535) + 1
          }
        }
      }
      await zarr.set(arr, null, {
        data,
        shape: [...shape],
        stride: [shape[1] * shape[2], shape[2], 1],
      })

      // SAB path
      const sabResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: true,
      })

      // Normal path (ground truth)
      const normalResult = await getWorker(arr, null, {
        pool,
        useSharedArrayBuffer: false,
      })

      pool.terminateWorkers()

      const sabData = sabResult.data as Uint16Array
      const normalData = normalResult.data as Uint16Array

      let mismatchCount = 0
      let firstMismatchIdx = -1
      for (let i = 0; i < totalSize; i++) {
        if (sabData[i] !== normalData[i]) {
          mismatchCount++
          if (firstMismatchIdx === -1) firstMismatchIdx = i
        }
      }

      return {
        match: mismatchCount === 0,
        mismatchCount,
        firstMismatchIdx,
        totalSize,
        isShared: sabResult.data.buffer instanceof SharedArrayBuffer,
        // Debug info around mismatch
        sabSlice: firstMismatchIdx >= 0
          ? Array.from(sabData.slice(firstMismatchIdx, firstMismatchIdx + 8))
          : [],
        normalSlice: firstMismatchIdx >= 0
          ? Array.from(normalData.slice(firstMismatchIdx, firstMismatchIdx + 8))
          : [],
        // Convert index to 3D coords for debugging
        mismatchCoords: firstMismatchIdx >= 0
          ? {
              z: Math.floor(firstMismatchIdx / (256 * 256)),
              y: Math.floor((firstMismatchIdx % (256 * 256)) / 256),
              x: firstMismatchIdx % 256,
            }
          : null,
      }
    })

    expect(result.isShared).toBe(true)
    if (!result.match) {
      const c = result.mismatchCoords
      throw new Error(
        `SAB decode_into: ${result.mismatchCount}/${result.totalSize} mismatches. ` +
        `First at flat index ${result.firstMismatchIdx} ` +
        `(z=${c?.z}, y=${c?.y}, x=${c?.x}). ` +
        `SAB: [${result.sabSlice}], Normal: [${result.normalSlice}]`
      )
    }
    expect(result.match).toBe(true)
  })

  test('SAB decode_into: slice selection crossing chunk boundaries with edge chunks', async ({ page }) => {
    // Shape [10, 6] with chunks [3, 4], reading a slice that crosses
    // chunk boundaries: [1:9, 1:5]
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const shape = [10, 6] as const
      const chunkShape = [3, 4] as const
      const totalSize = shape[0] * shape[1]

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [...shape],
        chunk_shape: [...chunkShape],
        data_type: 'float32',
      })

      const data = new Float32Array(totalSize)
      for (let i = 0; i < totalSize; i++) data[i] = i + 0.5
      await zarr.set(arr, null, {
        data,
        shape: [...shape],
        stride: [shape[1], 1],
      })

      // Read a slice that crosses boundaries: rows 1..8, cols 1..4
      const sel = [zarr.slice(1, 9), zarr.slice(1, 5)]

      const sabResult = await getWorker(arr, sel, {
        pool,
        useSharedArrayBuffer: true,
      })

      const normalResult = await getWorker(arr, sel, {
        pool,
        useSharedArrayBuffer: false,
      })

      pool.terminateWorkers()

      const sabData = Array.from(sabResult.data as Float32Array)
      const normalData = Array.from(normalResult.data as Float32Array)

      return {
        sabData,
        normalData,
        match: sabData.every((v, i) => v === normalData[i]),
        sabShape: sabResult.shape,
        normalShape: normalResult.shape,
        isShared: sabResult.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.isShared).toBe(true)
    expect(result.sabShape).toEqual(result.normalShape)
    expect(result.sabData).toEqual(result.normalData)
  })

  // -------------------------------------------------------------------------
  // Edge chunk tests — non-padded edge chunks (simulating external writers)
  // -------------------------------------------------------------------------
  // These tests simulate data written by external tools (e.g., Python zarr)
  // where edge chunks are stored at their actual smaller size, NOT padded
  // to the full chunk_shape. This is the spec-compliant behavior for zarr v3.
  // zarrita's set() always writes full-padded chunks, so these tests manually
  // write smaller raw chunk data directly to the store.

  test('edge chunks: 1D array with non-padded edge chunk', async ({ page }) => {
    // shape [7], chunk_shape [3] => chunks: [0..2], [3..5], [6]
    // The last chunk has only 1 element (not padded to 3).
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [7],
        chunk_shape: [3],
        data_type: 'int32',
      })

      // Write full chunks manually (3 elements each)
      const chunk0 = new Int32Array([10, 20, 30])
      const chunk1 = new Int32Array([40, 50, 60])
      // Edge chunk: only 1 element (NOT padded to 3)
      const chunk2 = new Int32Array([70])

      const mapStore = arr.store as unknown as {
        set(k: string, v: Uint8Array): void
      }
      mapStore.set('/c/0', new Uint8Array(chunk0.buffer))
      mapStore.set('/c/1', new Uint8Array(chunk1.buffer))
      mapStore.set('/c/2', new Uint8Array(chunk2.buffer))

      // Read full array using getWorker (non-SAB path)
      const chunk = await getWorker(arr, null, { pool })
      pool.terminateWorkers()

      return {
        data: Array.from(chunk.data as Int32Array),
        shape: chunk.shape,
      }
    })

    expect(result.shape).toEqual([7])
    expect(result.data).toEqual([10, 20, 30, 40, 50, 60, 70])
  })

  test('edge chunks: 3D array mimicking OME-Zarr with non-padded edge chunks', async ({ page }) => {
    // shape [5, 4, 6], chunk_shape [3, 4, 4]
    // dim 0: 2 chunks — [0..2] full (3), [3..4] edge (2)
    // dim 1: 1 chunk — [0..3] full (4)
    // dim 2: 2 chunks — [0..3] full (4), [4..5] edge (2)
    //
    // Chunks:
    // (0,0,0): shape [3,4,4] = 48 elements — full
    // (0,0,1): shape [3,4,2] = 24 elements — edge in dim 2
    // (1,0,0): shape [2,4,4] = 32 elements — edge in dim 0
    // (1,0,1): shape [2,4,2] = 16 elements — edge in dim 0 AND dim 2
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const arrayShape = [5, 4, 6]
      const chunkShape = [3, 4, 4]

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: arrayShape,
        chunk_shape: chunkShape,
        data_type: 'float32',
      })

      // Build expected output: sequential values in C-order
      const expected = new Float32Array(5 * 4 * 6)
      for (let i = 0; i < expected.length; i++) expected[i] = i + 1

      // Helper: extract a chunk from the full array at given chunk coords
      // and write it at its actual (non-padded) size
      function extractChunk(
        fullData: Float32Array,
        fullShape: number[],
        chunkCoords: number[],
        chunkShape: number[],
      ): Float32Array {
        const actualShape = chunkCoords.map((c, d) =>
          Math.min(chunkShape[d], fullShape[d] - c * chunkShape[d]),
        )
        const size = actualShape.reduce((a, b) => a * b, 1)
        const result = new Float32Array(size)
        let idx = 0
        for (let z = 0; z < actualShape[0]; z++) {
          for (let y = 0; y < actualShape[1]; y++) {
            for (let x = 0; x < actualShape[2]; x++) {
              const gz = chunkCoords[0] * chunkShape[0] + z
              const gy = chunkCoords[1] * chunkShape[1] + y
              const gx = chunkCoords[2] * chunkShape[2] + x
              result[idx++] = fullData[gz * fullShape[1] * fullShape[2] + gy * fullShape[2] + gx]
            }
          }
        }
        return result
      }

      const mapStore = arr.store as unknown as { set(k: string, v: Uint8Array): void }

      // Write each chunk at its actual (non-padded) size
      const nChunks = chunkShape.map((cs, d) => Math.ceil(arrayShape[d] / cs))
      for (let cz = 0; cz < nChunks[0]; cz++) {
        for (let cy = 0; cy < nChunks[1]; cy++) {
          for (let cx = 0; cx < nChunks[2]; cx++) {
            const data = extractChunk(expected, arrayShape, [cz, cy, cx], chunkShape)
            const key = `/c/${cz}/${cy}/${cx}`
            mapStore.set(key, new Uint8Array(data.buffer))
          }
        }
      }

      // Read using getWorker
      const chunk = await getWorker(arr, null, { pool })

      // Also read using SAB path
      const sabChunk = await getWorker(arr, null, { pool, useSharedArrayBuffer: true })

      pool.terminateWorkers()

      const workerData = Array.from(chunk.data as Float32Array)
      const sabData = Array.from(sabChunk.data as Float32Array)
      const expectedArr = Array.from(expected)

      return {
        workerData,
        sabData,
        expected: expectedArr,
        workerShape: chunk.shape,
        sabShape: sabChunk.shape,
        workerMatch: workerData.every((v, i) => v === expectedArr[i]),
        sabMatch: sabData.every((v, i) => v === expectedArr[i]),
        sabIsShared: sabChunk.data.buffer instanceof SharedArrayBuffer,
      }
    })

    expect(result.workerShape).toEqual([5, 4, 6])
    expect(result.sabShape).toEqual([5, 4, 6])
    expect(result.sabIsShared).toBe(true)
    expect(result.workerMatch).toBe(true)
    expect(result.sabMatch).toBe(true)
    expect(result.workerData).toEqual(result.expected)
    expect(result.sabData).toEqual(result.expected)
  })

  test('edge chunks: beechnut-like geometry with non-padded edge chunks', async ({ page }) => {
    // Mimics beechnut level 2: shape [386, 256, 256] chunks [96, 96, 96]
    // but smaller: shape [10, 7, 7] chunks [4, 4, 4]
    // dim 0: 3 chunks — [0..3], [4..7], [8..9] edge (2)
    // dim 1: 2 chunks — [0..3], [4..6] edge (3)
    // dim 2: 2 chunks — [0..3], [4..6] edge (3)
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(4)

      const arrayShape = [10, 7, 7]
      const chunkShape = [4, 4, 4]

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: arrayShape,
        chunk_shape: chunkShape,
        data_type: 'uint16',
      })

      // Build expected output
      const totalSize = arrayShape[0] * arrayShape[1] * arrayShape[2]
      const expected = new Uint16Array(totalSize)
      for (let i = 0; i < totalSize; i++) expected[i] = (i % 65535) + 1

      // Helper: extract chunk data at actual (non-padded) size
      function extractChunk(
        fullData: Uint16Array,
        fullShape: number[],
        chunkCoords: number[],
        chunkShape: number[],
      ): Uint16Array {
        const actualShape = chunkCoords.map((c, d) =>
          Math.min(chunkShape[d], fullShape[d] - c * chunkShape[d]),
        )
        const size = actualShape.reduce((a, b) => a * b, 1)
        const result = new Uint16Array(size)
        let idx = 0
        for (let z = 0; z < actualShape[0]; z++) {
          for (let y = 0; y < actualShape[1]; y++) {
            for (let x = 0; x < actualShape[2]; x++) {
              const gz = chunkCoords[0] * chunkShape[0] + z
              const gy = chunkCoords[1] * chunkShape[1] + y
              const gx = chunkCoords[2] * chunkShape[2] + x
              result[idx++] = fullData[gz * fullShape[1] * fullShape[2] + gy * fullShape[2] + gx]
            }
          }
        }
        return result
      }

      const mapStore = arr.store as unknown as { set(k: string, v: Uint8Array): void }

      const nChunks = chunkShape.map((cs, d) => Math.ceil(arrayShape[d] / cs))
      for (let cz = 0; cz < nChunks[0]; cz++) {
        for (let cy = 0; cy < nChunks[1]; cy++) {
          for (let cx = 0; cx < nChunks[2]; cx++) {
            const data = extractChunk(expected, arrayShape, [cz, cy, cx], chunkShape)
            const key = `/c/${cz}/${cy}/${cx}`
            mapStore.set(key, new Uint8Array(data.buffer.slice(0)))
          }
        }
      }

      // Read using getWorker (non-SAB)
      const chunk = await getWorker(arr, null, { pool })

      // Read using getWorker (SAB)
      const sabChunk = await getWorker(arr, null, { pool, useSharedArrayBuffer: true })

      pool.terminateWorkers()

      const workerData = Array.from(chunk.data as Uint16Array)
      const sabData = Array.from(sabChunk.data as Uint16Array)
      const expectedArr = Array.from(expected)

      // Find first mismatch for debug
      let workerMismatchIdx = -1
      let sabMismatchIdx = -1
      for (let i = 0; i < totalSize; i++) {
        if (workerData[i] !== expectedArr[i] && workerMismatchIdx === -1) workerMismatchIdx = i
        if (sabData[i] !== expectedArr[i] && sabMismatchIdx === -1) sabMismatchIdx = i
      }

      return {
        workerMatch: workerData.every((v, i) => v === expectedArr[i]),
        sabMatch: sabData.every((v, i) => v === expectedArr[i]),
        workerMismatchIdx,
        sabMismatchIdx,
        totalSize,
        workerShape: chunk.shape,
        sabShape: sabChunk.shape,
        sabIsShared: sabChunk.data.buffer instanceof SharedArrayBuffer,
        // Debug slices around first mismatch
        workerSlice: workerMismatchIdx >= 0
          ? workerData.slice(workerMismatchIdx, workerMismatchIdx + 8)
          : [],
        expectedSlice: workerMismatchIdx >= 0
          ? expectedArr.slice(workerMismatchIdx, workerMismatchIdx + 8)
          : [],
      }
    })

    expect(result.workerShape).toEqual([10, 7, 7])
    expect(result.sabShape).toEqual([10, 7, 7])
    expect(result.sabIsShared).toBe(true)

    if (!result.workerMatch) {
      throw new Error(
        `Non-SAB edge chunk mismatch at index ${result.workerMismatchIdx}/${result.totalSize}. ` +
        `Got: [${result.workerSlice}], Expected: [${result.expectedSlice}]`
      )
    }
    if (!result.sabMatch) {
      throw new Error(
        `SAB edge chunk mismatch at index ${result.sabMismatchIdx}/${result.totalSize}.`
      )
    }
    expect(result.workerMatch).toBe(true)
    expect(result.sabMatch).toBe(true)
  })

  // -------------------------------------------------------------------------
  // Chunk caching
  // -------------------------------------------------------------------------

  test('getWorker works without cache (default, unchanged behavior)', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'int32',
      })

      const data = new Int32Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
      await zarr.set(arr, null, { data, shape: [4, 4], stride: [4, 1] })

      // No cache option — default behavior
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

  test('getWorker with Map cache returns correct data and populates cache', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'int32',
      })

      const data = new Int32Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
      await zarr.set(arr, null, { data, shape: [4, 4], stride: [4, 1] })

      const cache = new Map()

      // First call — populates cache
      const chunk1 = await getWorker(arr, null, { pool, cache })
      const cacheSize = cache.size

      // Second call — uses cache
      const chunk2 = await getWorker(arr, null, { pool, cache })

      pool.terminateWorkers()

      return {
        data1: Array.from(chunk1.data as Int32Array),
        data2: Array.from(chunk2.data as Int32Array),
        shape: chunk1.shape,
        cacheSizeAfterFirst: cacheSize,
        cacheSizeAfterSecond: cache.size,
      }
    })

    expect(result.data1).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    expect(result.data2).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    expect(result.shape).toEqual([4, 4])
    // 4 chunks for a [4,4] array with [2,2] chunk shape
    expect(result.cacheSizeAfterFirst).toBe(4)
    expect(result.cacheSizeAfterSecond).toBe(4)
  })

  test('cache keys use store_N:/path:chunkKey format', async ({ page }) => {
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
        data: new Int32Array([10, 20, 30, 40]),
        shape: [4],
        stride: [1],
      })

      const cache = new Map()
      await getWorker(arr, null, { pool, cache })
      pool.terminateWorkers()

      return {
        keys: Array.from(cache.keys()),
      }
    })

    expect(result.keys.length).toBe(2)
    for (const key of result.keys) {
      // Pattern: store_N:/path:c/digit  (path may be just "/")
      expect(key).toMatch(/^store_\d+:\/[^:]*:c\/\d+$/)
    }
  })

  test('cached reads skip chunk fetches on second call', async ({ page }) => {
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
        data: new Int32Array([10, 20, 30, 40]),
        shape: [4],
        stride: [1],
      })

      // Instrument store.get to track chunk-key reads (c/0, c/1, etc.)
      const originalGet = arr.store.get.bind(arr.store)
      const chunkPaths: string[] = []
      ;(arr.store as any).get = (path: string, ...rest: any[]) => {
        if (path.includes('/c/')) chunkPaths.push(path)
        return originalGet(path, ...rest)
      }

      const cache = new Map()

      // First call — cache miss, triggers chunk reads (probe + 2 data chunks)
      await getWorker(arr, null, { pool, cache })
      const firstCallChunkReads = chunkPaths.length

      // Reset tracker
      chunkPaths.length = 0

      // Second call — all chunks cached, only the shape probe hits the store
      await getWorker(arr, null, { pool, cache })
      const secondCallChunkReads = chunkPaths.length

      pool.terminateWorkers()

      return {
        firstCallChunkReads,
        secondCallChunkReads,
      }
    })

    // First call reads chunks: 1 probe + 2 data chunks = 3
    expect(result.firstCallChunkReads).toBeGreaterThanOrEqual(2)
    // Second call: only the shape probe read, NO data chunk reads
    // (probe reads c/0 for shape detection, but no per-chunk task reads)
    expect(result.secondCallChunkReads).toBeLessThan(result.firstCallChunkReads)
  })

  test('custom cache implementation receives get/set calls', async ({ page }) => {
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
        data: new Int32Array([10, 20, 30, 40]),
        shape: [4],
        stride: [1],
      })

      // Custom cache that tracks operations
      const ops: string[] = []
      const backing = new Map()
      const cache = {
        get(key: string) {
          ops.push(`get:${key}`)
          return backing.get(key)
        },
        set(key: string, value: any) {
          ops.push(`set:${key}`)
          backing.set(key, value)
        },
      }

      // First call: get (miss) then set
      await getWorker(arr, null, { pool, cache })
      const opsAfterFirst = [...ops]

      // Second call: get (hit)
      await getWorker(arr, null, { pool, cache })

      pool.terminateWorkers()

      return {
        opsAfterFirst,
        opsAll: ops,
        hasGetOps: ops.some(o => o.startsWith('get:')),
        hasSetOps: ops.some(o => o.startsWith('set:')),
      }
    })

    expect(result.hasGetOps).toBe(true)
    expect(result.hasSetOps).toBe(true)
    // First call: 2 get misses + 2 set calls = 4 ops
    expect(result.opsAfterFirst.length).toBe(4)
    // Second call: 2 get hits (no sets since already cached)
    expect(result.opsAll.length).toBe(6)
  })

  test('cache with sliced access shares cached chunks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'int32',
      })

      const data = new Int32Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
      await zarr.set(arr, null, { data, shape: [4, 4], stride: [4, 1] })

      const cache = new Map()

      // Access first two rows (overlaps with chunks at row indices 0..1)
      const slice1 = await getWorker(arr, [zarr.slice(0, 2), null], { pool, cache })
      const cacheAfterSlice1 = cache.size

      // Access last two rows (overlaps with chunks at row indices 2..3)
      const slice2 = await getWorker(arr, [zarr.slice(2, 4), null], { pool, cache })
      const cacheAfterSlice2 = cache.size

      // Access all rows — all chunks should be cached now
      const full = await getWorker(arr, null, { pool, cache })
      const cacheAfterFull = cache.size

      pool.terminateWorkers()

      return {
        slice1Data: Array.from(slice1.data as Int32Array),
        slice1Shape: slice1.shape,
        slice2Data: Array.from(slice2.data as Int32Array),
        slice2Shape: slice2.shape,
        fullData: Array.from(full.data as Int32Array),
        cacheAfterSlice1,
        cacheAfterSlice2,
        cacheAfterFull,
      }
    })

    expect(result.slice1Data).toEqual([1, 2, 3, 4, 5, 6, 7, 8])
    expect(result.slice1Shape).toEqual([2, 4])
    expect(result.slice2Data).toEqual([9, 10, 11, 12, 13, 14, 15, 16])
    expect(result.slice2Shape).toEqual([2, 4])
    expect(result.fullData).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    // First slice touches 2 chunks, second slice adds 2 more = 4 total
    expect(result.cacheAfterSlice1).toBe(2)
    expect(result.cacheAfterSlice2).toBe(4)
    // Full access should not add more cache entries (all 4 already cached)
    expect(result.cacheAfterFull).toBe(4)
  })

  test('different arrays have separate cache entries', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()

      const arr1 = await zarr.create(store.resolve('/arr1'), {
        shape: [4],
        chunk_shape: [4],
        data_type: 'int32',
      })
      await zarr.set(arr1, null, {
        data: new Int32Array([1, 2, 3, 4]),
        shape: [4],
        stride: [1],
      })

      const arr2 = await zarr.create(store.resolve('/arr2'), {
        shape: [4],
        chunk_shape: [4],
        data_type: 'int32',
      })
      await zarr.set(arr2, null, {
        data: new Int32Array([10, 20, 30, 40]),
        shape: [4],
        stride: [1],
      })

      const cache = new Map()

      await getWorker(arr1, null, { pool, cache })
      await getWorker(arr2, null, { pool, cache })

      const keys = Array.from(cache.keys())

      pool.terminateWorkers()

      return {
        keys,
        cacheSize: cache.size,
        hasArr1Key: keys.some(k => k.includes('/arr1:')),
        hasArr2Key: keys.some(k => k.includes('/arr2:')),
      }
    })

    expect(result.cacheSize).toBe(2)
    expect(result.hasArr1Key).toBe(true)
    expect(result.hasArr2Key).toBe(true)
  })

  test('different stores have separate cache entries (store ID isolation)', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      // Two separate stores
      const store1 = zarr.root()
      const arr1 = await zarr.create(store1, {
        shape: [4],
        chunk_shape: [4],
        data_type: 'int32',
      })
      await zarr.set(arr1, null, {
        data: new Int32Array([1, 2, 3, 4]),
        shape: [4],
        stride: [1],
      })

      const store2 = zarr.root()
      const arr2 = await zarr.create(store2, {
        shape: [4],
        chunk_shape: [4],
        data_type: 'int32',
      })
      await zarr.set(arr2, null, {
        data: new Int32Array([10, 20, 30, 40]),
        shape: [4],
        stride: [1],
      })

      const cache = new Map()

      await getWorker(arr1, null, { pool, cache })
      await getWorker(arr2, null, { pool, cache })

      const keys = Array.from(cache.keys()) as string[]

      pool.terminateWorkers()

      // Extract store ID prefixes
      const storePrefixes = new Set(keys.map(k => k.split(':')[0]))

      return {
        cacheSize: cache.size,
        numStorePrefixes: storePrefixes.size,
        keys,
      }
    })

    expect(result.cacheSize).toBe(2)
    // Two different stores should have different store_N prefixes
    expect(result.numStorePrefixes).toBe(2)
  })

  test('getWorker with cache + useSharedArrayBuffer returns correct SAB-backed data', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'int32',
      })

      const data = new Int32Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
      await zarr.set(arr, null, { data, shape: [4, 4], stride: [4, 1] })

      const cache = new Map()

      // First call with SAB + cache — cache miss, should decode and cache
      const chunk1 = await getWorker(arr, null, { pool, cache, useSharedArrayBuffer: true })
      const cacheSize = cache.size

      // Verify the output is backed by SharedArrayBuffer
      const isShared1 = (chunk1.data as Int32Array).buffer instanceof SharedArrayBuffer

      // Second call with SAB + cache — cache hit, should skip worker
      const chunk2 = await getWorker(arr, null, { pool, cache, useSharedArrayBuffer: true })
      const isShared2 = (chunk2.data as Int32Array).buffer instanceof SharedArrayBuffer

      pool.terminateWorkers()

      return {
        data1: Array.from(chunk1.data as Int32Array),
        data2: Array.from(chunk2.data as Int32Array),
        shape1: chunk1.shape,
        shape2: chunk2.shape,
        cacheSize,
        isShared1,
        isShared2,
      }
    })

    expect(result.data1).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    expect(result.data2).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16])
    expect(result.shape1).toEqual([4, 4])
    expect(result.shape2).toEqual([4, 4])
    expect(result.cacheSize).toBe(4)
    expect(result.isShared1).toBe(true)
    expect(result.isShared2).toBe(true)
  })

  test('getWorker with cache and gzip-compressed chunks', async ({ page }) => {
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
      const compressedStream = new Response(rawBytes).body!
        .pipeThrough(new CompressionStream('gzip'))
      const compressedBytes = new Uint8Array(
        await new Response(compressedStream).arrayBuffer()
      )
      const mapStore = arr.store as unknown as { set(k: string, v: Uint8Array): void }
      mapStore.set('/c/0', compressedBytes)

      const cache = new Map()

      // First call — decompresses, populates cache
      const chunk1 = await getWorker(arr, null, { pool, cache })

      // Second call — should use cache, skip decompression
      const chunk2 = await getWorker(arr, null, { pool, cache })

      pool.terminateWorkers()

      return {
        data1: Array.from(chunk1.data as Int32Array),
        data2: Array.from(chunk2.data as Int32Array),
        cacheSize: cache.size,
      }
    })

    expect(result.data1).toEqual([100, 200, 300, 400])
    expect(result.data2).toEqual([100, 200, 300, 400])
    expect(result.cacheSize).toBe(1)
  })

  test('cache with fill-value chunks (missing data)', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { zarr, getWorker, WorkerPool } = window
      const pool = new WorkerPool(2)

      const store = zarr.root()
      const arr = await zarr.create(store, {
        shape: [4],
        chunk_shape: [2],
        data_type: 'int32',
        fill_value: 42,
      })

      // Write data for only the first chunk, leave second as missing
      await zarr.set(arr, [zarr.slice(0, 2)], {
        data: new Int32Array([10, 20]),
        shape: [2],
        stride: [1],
      })

      const cache = new Map()

      const chunk1 = await getWorker(arr, null, { pool, cache })
      const cacheSize = cache.size

      // Second call should use cache
      const chunk2 = await getWorker(arr, null, { pool, cache })

      pool.terminateWorkers()

      return {
        data1: Array.from(chunk1.data as Int32Array),
        data2: Array.from(chunk2.data as Int32Array),
        cacheSize,
        cacheSizeAfterSecond: cache.size,
      }
    })

    // First chunk has written data, second chunk should have fill value 42
    expect(result.data1).toEqual([10, 20, 42, 42])
    expect(result.data2).toEqual([10, 20, 42, 42])
    expect(result.cacheSize).toBe(2)
    expect(result.cacheSizeAfterSecond).toBe(2)
  })

})
