/**
 * Integration tests exercising the WorkerPool with zarrita-compatible
 * get/set operations.
 *
 * These tests port the core zarrita.js test patterns from:
 *   - packages/zarrita/__tests__/indexing/set.test.ts
 *   - packages/zarrita/__tests__/indexing/slice.test.ts
 *
 * They verify that the WorkerPool correctly schedules chunk-level I/O
 * when used as the ChunkQueue provider for zarrita-style get/set.
 */

import { test, expect } from '@playwright/test'

test.describe('zarrita get/set with default queue (baseline)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // -------------------------------------------------------------------------
  // From set.test.ts: "Read and write array data - builtin"
  // -------------------------------------------------------------------------

  test('basic get/set round-trip on [5,10] array', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { ZarrArray, zarrGet, zarrSet, zarrSlice, zarrRange } = window

      const arr = new ZarrArray({
        shape: [5, 10],
        chunk_shape: [2, 5],
        data_type: 'int32' as const,
      })

      // Read initial zeros
      let res = await zarrGet(arr, [null, null]) as { data: Int32Array; shape: number[] }
      const initialShape = res.shape
      const initialData = Array.from(res.data)

      // Set first row to 42
      await zarrSet(arr, [0, null], 42)
      res = await zarrGet(arr, null) as { data: Int32Array; shape: number[] }
      const afterRow0 = Array.from(res.data)

      // Set column 0 to 42
      await zarrSet(arr, [null, 0], 42)
      res = await zarrGet(arr, null) as { data: Int32Array; shape: number[] }
      const afterCol0 = Array.from(res.data)

      // Set entire array to 42
      await zarrSet(arr, null, 42)
      res = await zarrGet(arr, null) as { data: Int32Array; shape: number[] }
      const afterAll42 = Array.from(res.data)

      // Set first row to [0,1,2,...,9]
      const rowData = {
        data: new Int32Array(Array.from(zarrRange(10))),
        shape: [10],
        stride: [1],
      }
      await zarrSet(arr, [0, null], rowData)
      res = await zarrGet(arr, null) as { data: Int32Array; shape: number[] }
      const afterRowChunk = Array.from(res.data)

      // Set entire array to [0..49]
      const fullData = {
        data: new Int32Array(Array.from(zarrRange(50))),
        shape: [5, 10],
        stride: [10, 1],
      }
      await zarrSet(arr, null, fullData)
      res = await zarrGet(arr, null) as { data: Int32Array; shape: number[] }
      const afterFull = Array.from(res.data)

      // Read slices
      const sliceCol0 = await zarrGet(arr, [null, 0]) as { data: Int32Array; shape: number[] }
      const sliceCol1 = await zarrGet(arr, [null, 1]) as { data: Int32Array; shape: number[] }
      const sliceRow0 = await zarrGet(arr, [0, null]) as { data: Int32Array; shape: number[] }
      const sliceRow1 = await zarrGet(arr, [1, null]) as { data: Int32Array; shape: number[] }

      const sliceCols07 = await zarrGet(arr, [null, zarrSlice(0, 7)]) as { data: Int32Array; shape: number[] }
      const sliceRows03 = await zarrGet(arr, [zarrSlice(0, 3), null]) as { data: Int32Array; shape: number[] }
      const sliceCross = await zarrGet(arr, [zarrSlice(0, 3), zarrSlice(0, 7)]) as { data: Int32Array; shape: number[] }
      const sliceInner = await zarrGet(arr, [zarrSlice(1, 4), zarrSlice(2, 7)]) as { data: Int32Array; shape: number[] }

      return {
        initialShape,
        initialData,
        afterRow0,
        afterCol0,
        afterAll42,
        afterRowChunk,
        afterFull,
        sliceCol0: { data: Array.from(sliceCol0.data), shape: sliceCol0.shape },
        sliceCol1: { data: Array.from(sliceCol1.data), shape: sliceCol1.shape },
        sliceRow0: { data: Array.from(sliceRow0.data), shape: sliceRow0.shape },
        sliceRow1: { data: Array.from(sliceRow1.data), shape: sliceRow1.shape },
        sliceCols07: { data: Array.from(sliceCols07.data), shape: sliceCols07.shape },
        sliceRows03: { data: Array.from(sliceRows03.data), shape: sliceRows03.shape },
        sliceCross: { data: Array.from(sliceCross.data), shape: sliceCross.shape },
        sliceInner: { data: Array.from(sliceInner.data), shape: sliceInner.shape },
      }
    })

    expect(result.initialShape).toEqual([5, 10])
    expect(result.initialData).toEqual(new Array(50).fill(0))

    // After setting row 0 to 42
    const expectedAfterRow0 = new Array(50).fill(0)
    expectedAfterRow0.fill(42, 0, 10)
    expect(result.afterRow0).toEqual(expectedAfterRow0)

    // After setting column 0 to 42
    const expectedAfterCol0 = [...expectedAfterRow0]
    for (const i of [10, 20, 30, 40]) expectedAfterCol0[i] = 42
    expect(result.afterCol0).toEqual(expectedAfterCol0)

    // After setting all to 42
    expect(result.afterAll42).toEqual(new Array(50).fill(42))

    // After setting row 0 to [0..9]
    const expectedAfterRowChunk = new Array(50).fill(42)
    for (let i = 0; i < 10; i++) expectedAfterRowChunk[i] = i
    expect(result.afterRowChunk).toEqual(expectedAfterRowChunk)

    // After setting full array to [0..49]
    expect(result.afterFull).toEqual(Array.from({ length: 50 }, (_, i) => i))

    // Slice reads
    expect(result.sliceCol0).toEqual({ data: [0, 10, 20, 30, 40], shape: [5] })
    expect(result.sliceCol1).toEqual({ data: [1, 11, 21, 31, 41], shape: [5] })
    expect(result.sliceRow0).toEqual({ data: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], shape: [10] })
    expect(result.sliceRow1).toEqual({ data: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], shape: [10] })

    expect(result.sliceCols07.shape).toEqual([5, 7])
    expect(result.sliceCols07.data).toEqual([
      0, 1, 2, 3, 4, 5, 6,
      10, 11, 12, 13, 14, 15, 16,
      20, 21, 22, 23, 24, 25, 26,
      30, 31, 32, 33, 34, 35, 36,
      40, 41, 42, 43, 44, 45, 46,
    ])

    expect(result.sliceRows03.shape).toEqual([3, 10])
    expect(result.sliceRows03.data).toEqual([
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
      10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
      20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    ])

    expect(result.sliceCross.shape).toEqual([3, 7])
    expect(result.sliceCross.data).toEqual([
      0, 1, 2, 3, 4, 5, 6,
      10, 11, 12, 13, 14, 15, 16,
      20, 21, 22, 23, 24, 25, 26,
    ])

    expect(result.sliceInner.shape).toEqual([3, 5])
    expect(result.sliceInner.data).toEqual([
      12, 13, 14, 15, 16,
      22, 23, 24, 25, 26,
      32, 33, 34, 35, 36,
    ])
  })

  // -------------------------------------------------------------------------
  // Large sparse array (from set.test.ts)
  // -------------------------------------------------------------------------

  test('large sparse array [7500000] with partial writes', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { ZarrArray, zarrGet, zarrSet, zarrSlice } = window

      const arr = new ZarrArray({
        shape: [7500000],
        chunk_shape: [42],
        data_type: 'float64' as const,
      })

      let res = await zarrGet(arr, [zarrSlice(10)]) as { data: Float64Array; shape: number[] }
      const initial = Array.from(res.data)

      await zarrSet(arr, [zarrSlice(5)], 1)
      res = await zarrGet(arr, [zarrSlice(10)]) as { data: Float64Array; shape: number[] }
      const afterSet = Array.from(res.data)

      return { initial, afterSet, shape: res.shape }
    })

    expect(result.shape).toEqual([10])
    expect(result.initial).toEqual(new Array(10).fill(0))
    expect(result.afterSet).toEqual([1, 1, 1, 1, 1, 0, 0, 0, 0, 0])
  })
})

test.describe('zarrita slice tests with default queue (baseline)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // -------------------------------------------------------------------------
  // From slice.test.ts: full array reads
  // -------------------------------------------------------------------------

  test('reads entire [2,3,4] array with various selection forms', async ({ page }) => {
    const DATA = {
      data: Array.from({ length: 24 }, (_, i) => i),
      shape: [2, 3, 4],
      stride: [12, 4, 1],
    }

    const result = await page.evaluate(async () => {
      const { ZarrArray, zarrGet, zarrSet, zarrSlice } = window

      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })

      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }
      await zarrSet(arr, null, DATA)

      const selections = [
        undefined,
        null,
        [null, null, null],
        [zarrSlice(null), zarrSlice(null), zarrSlice(null)],
        [null, zarrSlice(null), zarrSlice(null)],
      ]

      const results = []
      for (const sel of selections) {
        const res = await zarrGet(arr, sel as any) as { data: Int32Array; shape: number[]; stride: number[] }
        results.push({
          data: Array.from(res.data),
          shape: res.shape,
          stride: res.stride,
        })
      }
      return results
    })

    for (const res of result) {
      expect(res.data).toEqual(DATA.data)
      expect(res.shape).toEqual(DATA.shape)
      expect(res.stride).toEqual(DATA.stride)
    }
  })

  // -------------------------------------------------------------------------
  // From slice.test.ts: fancy slices
  // -------------------------------------------------------------------------

  test('reads fancy slices from [2,3,4] array', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { ZarrArray, zarrGet, zarrSet, zarrSlice } = window

      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })
      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }
      await zarrSet(arr, null, DATA)

      const testCases: Array<{ sel: any; expected: { data: number[]; shape: number[]; stride: number[] } }> = [
        {
          sel: [1, zarrSlice(1, 3), null],
          expected: {
            data: [16, 17, 18, 19, 20, 21, 22, 23],
            shape: [2, 4],
            stride: [4, 1],
          },
        },
        {
          sel: [null, zarrSlice(1, 3), zarrSlice(2)],
          expected: {
            data: [4, 5, 8, 9, 16, 17, 20, 21],
            shape: [2, 2, 2],
            stride: [4, 2, 1],
          },
        },
        {
          sel: [null, null, 0],
          expected: {
            data: [0, 4, 8, 12, 16, 20],
            shape: [2, 3],
            stride: [3, 1],
          },
        },
        {
          sel: [zarrSlice(3), zarrSlice(2), zarrSlice(2)],
          expected: {
            data: [0, 1, 4, 5, 12, 13, 16, 17],
            shape: [2, 2, 2],
            stride: [4, 2, 1],
          },
        },
        {
          sel: [1, null, null],
          expected: {
            data: [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23],
            shape: [3, 4],
            stride: [4, 1],
          },
        },
        {
          sel: [0, zarrSlice(null, null, 2), zarrSlice(null, null, 3)],
          expected: {
            data: [0, 3, 8, 11],
            shape: [2, 2],
            stride: [2, 1],
          },
        },
        {
          sel: [null, null, zarrSlice(null, null, 4)],
          expected: {
            data: [0, 4, 8, 12, 16, 20],
            shape: [2, 3, 1],
            stride: [3, 1, 1],
          },
        },
        {
          sel: [1, null, zarrSlice(null, 3, 2)],
          expected: {
            data: [12, 14, 16, 18, 20, 22],
            shape: [3, 2],
            stride: [2, 1],
          },
        },
        {
          sel: [null, 1, null],
          expected: {
            data: [4, 5, 6, 7, 16, 17, 18, 19],
            shape: [2, 4],
            stride: [4, 1],
          },
        },
        {
          sel: [1, 2, null],
          expected: {
            data: [20, 21, 22, 23],
            shape: [4],
            stride: [1],
          },
        },
      ]

      const results = []
      for (const { sel, expected } of testCases) {
        const res = await zarrGet(arr, sel) as { data: Int32Array; shape: number[]; stride: number[] }
        results.push({
          actual: { data: Array.from(res.data), shape: res.shape, stride: res.stride },
          expected,
        })
      }
      return results
    })

    for (const { actual, expected } of result) {
      expect(actual.data).toEqual(expected.data)
      expect(actual.shape).toEqual(expected.shape)
      expect(actual.stride).toEqual(expected.stride)
    }
  })

  // -------------------------------------------------------------------------
  // From slice.test.ts: scalar read
  // -------------------------------------------------------------------------

  test('reads a scalar from [2,3,4] array', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { ZarrArray, zarrGet, zarrSet } = window

      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })
      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }
      await zarrSet(arr, null, DATA)

      return zarrGet(arr, [1, 1, 1])
    })

    expect(result).toBe(17)
  })

  // -------------------------------------------------------------------------
  // From slice.test.ts: negative step rejection
  // -------------------------------------------------------------------------

  test('rejects negative step in slice', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { ZarrArray, zarrGet, zarrSet, zarrSlice, IndexError } = window

      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })
      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }
      await zarrSet(arr, null, DATA)

      try {
        await zarrGet(arr, [0, zarrSlice(null, null, -2), zarrSlice(null, null, 3)])
        return { threw: false, name: '' }
      } catch (e: any) {
        return { threw: true, name: e.name }
      }
    })

    expect(result.threw).toBe(true)
    expect(result.name).toBe('IndexError')
  })
})

// ===========================================================================
// Worker Pool Integration Tests
// ===========================================================================

test.describe('zarrita get/set with WorkerPool queue', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // -------------------------------------------------------------------------
  // Basic round-trip via WorkerPool
  // -------------------------------------------------------------------------

  test('get/set round-trip using WorkerPool queue', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)

      const arr = new ZarrArray({
        shape: [5, 10],
        chunk_shape: [2, 5],
        data_type: 'int32' as const,
      })

      // Write data via pool
      const fullData = {
        data: new Int32Array(Array.from({ length: 50 }, (_, i) => i)),
        shape: [5, 10],
        stride: [10, 1],
      }
      await zarrSet(arr, null, fullData, workerPoolSetOptions(pool, arr))

      // Read back via pool
      const res = await zarrGet(arr, null, workerPoolGetOptions(pool, arr)) as { data: Int32Array; shape: number[] }

      pool.terminateWorkers()

      return {
        data: Array.from(res.data),
        shape: res.shape,
      }
    })

    expect(result.shape).toEqual([5, 10])
    expect(result.data).toEqual(Array.from({ length: 50 }, (_, i) => i))
  })

  // -------------------------------------------------------------------------
  // Slice reads via WorkerPool
  // -------------------------------------------------------------------------

  test('slice reads using WorkerPool queue', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, zarrSlice, workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })

      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }

      // Write via pool
      await zarrSet(arr, null, DATA, workerPoolSetOptions(pool, arr))

      const getOpts = workerPoolGetOptions(pool, arr)

      // Full read
      const full = await zarrGet(arr, null, getOpts) as { data: Int32Array; shape: number[]; stride: number[] }

      // Fancy slice
      const poolOpts2 = workerPoolGetOptions(pool, arr)
      const sliced = await zarrGet(arr, [1, zarrSlice(1, 3), null], poolOpts2) as { data: Int32Array; shape: number[]; stride: number[] }

      // Integer indexing on all dims -> scalar
      const poolOpts3 = workerPoolGetOptions(pool, arr)
      const scalar = await zarrGet(arr, [1, 1, 1], poolOpts3)

      pool.terminateWorkers()

      return {
        full: { data: Array.from(full.data), shape: full.shape, stride: full.stride },
        sliced: { data: Array.from(sliced.data), shape: sliced.shape, stride: sliced.stride },
        scalar,
      }
    })

    expect(result.full.data).toEqual(Array.from({ length: 24 }, (_, i) => i))
    expect(result.full.shape).toEqual([2, 3, 4])
    expect(result.full.stride).toEqual([12, 4, 1])

    expect(result.sliced.data).toEqual([16, 17, 18, 19, 20, 21, 22, 23])
    expect(result.sliced.shape).toEqual([2, 4])
    expect(result.sliced.stride).toEqual([4, 1])

    expect(result.scalar).toBe(17)
  })

  // -------------------------------------------------------------------------
  // Full zarrita set.test.ts via WorkerPool
  // -------------------------------------------------------------------------

  test('full set.test.ts pattern via WorkerPool', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, zarrSlice, zarrRange,
              workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [5, 10],
        chunk_shape: [2, 5],
        data_type: 'int32' as const,
      })

      const getOpts = () => workerPoolGetOptions(pool, arr)
      const setOpts = () => workerPoolSetOptions(pool, arr)

      // Set row 0 to 42
      await zarrSet(arr, [0, null], 42, setOpts())
      let res = await zarrGet(arr, null, getOpts()) as { data: Int32Array; shape: number[] }
      const afterRow0 = Array.from(res.data)

      // Set col 0 to 42
      await zarrSet(arr, [null, 0], 42, setOpts())
      res = await zarrGet(arr, null, getOpts()) as { data: Int32Array; shape: number[] }
      const afterCol0 = Array.from(res.data)

      // Set all to 42
      await zarrSet(arr, null, 42, setOpts())
      res = await zarrGet(arr, null, getOpts()) as { data: Int32Array; shape: number[] }
      const afterAll = Array.from(res.data)

      // Set full data
      const fullData = {
        data: new Int32Array(Array.from(zarrRange(50))),
        shape: [5, 10],
        stride: [10, 1],
      }
      await zarrSet(arr, null, fullData, setOpts())

      // Slice reads
      const inner = await zarrGet(arr, [zarrSlice(1, 4), zarrSlice(2, 7)], getOpts()) as { data: Int32Array; shape: number[] }

      pool.terminateWorkers()

      return {
        afterRow0,
        afterCol0,
        afterAll,
        inner: { data: Array.from(inner.data), shape: inner.shape },
      }
    })

    const expectedAfterRow0 = new Array(50).fill(0)
    expectedAfterRow0.fill(42, 0, 10)
    expect(result.afterRow0).toEqual(expectedAfterRow0)

    const expectedAfterCol0 = [...expectedAfterRow0]
    for (const i of [10, 20, 30, 40]) expectedAfterCol0[i] = 42
    expect(result.afterCol0).toEqual(expectedAfterCol0)

    expect(result.afterAll).toEqual(new Array(50).fill(42))

    expect(result.inner.shape).toEqual([3, 5])
    expect(result.inner.data).toEqual([
      12, 13, 14, 15, 16,
      22, 23, 24, 25, 26,
      32, 33, 34, 35, 36,
    ])
  })

  // -------------------------------------------------------------------------
  // Full slice.test.ts fancy slices via WorkerPool
  // -------------------------------------------------------------------------

  test('all fancy slices from slice.test.ts via WorkerPool', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, zarrSlice,
              workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })

      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }
      await zarrSet(arr, null, DATA, workerPoolSetOptions(pool, arr))

      const testCases: Array<{ sel: any; key: string }> = [
        { sel: [1, zarrSlice(1, 3), null], key: 'a' },
        { sel: [null, zarrSlice(1, 3), zarrSlice(2)], key: 'b' },
        { sel: [null, null, 0], key: 'c' },
        { sel: [zarrSlice(3), zarrSlice(2), zarrSlice(2)], key: 'd' },
        { sel: [1, null, null], key: 'e' },
        { sel: [0, zarrSlice(null, null, 2), zarrSlice(null, null, 3)], key: 'f' },
        { sel: [null, null, zarrSlice(null, null, 4)], key: 'g' },
        { sel: [1, null, zarrSlice(null, 3, 2)], key: 'h' },
        { sel: [null, 1, null], key: 'i' },
        { sel: [1, 2, null], key: 'j' },
      ]

      const results: Record<string, { data: number[]; shape: number[]; stride: number[] }> = {}
      for (const { sel, key } of testCases) {
        const opts = workerPoolGetOptions(pool, arr)
        const res = await zarrGet(arr, sel, opts) as { data: Int32Array; shape: number[]; stride: number[] }
        results[key] = { data: Array.from(res.data), shape: res.shape, stride: res.stride }
      }

      pool.terminateWorkers()
      return results
    })

    expect(result.a).toEqual({ data: [16, 17, 18, 19, 20, 21, 22, 23], shape: [2, 4], stride: [4, 1] })
    expect(result.b).toEqual({ data: [4, 5, 8, 9, 16, 17, 20, 21], shape: [2, 2, 2], stride: [4, 2, 1] })
    expect(result.c).toEqual({ data: [0, 4, 8, 12, 16, 20], shape: [2, 3], stride: [3, 1] })
    expect(result.d).toEqual({ data: [0, 1, 4, 5, 12, 13, 16, 17], shape: [2, 2, 2], stride: [4, 2, 1] })
    expect(result.e).toEqual({ data: [12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23], shape: [3, 4], stride: [4, 1] })
    expect(result.f).toEqual({ data: [0, 3, 8, 11], shape: [2, 2], stride: [2, 1] })
    expect(result.g).toEqual({ data: [0, 4, 8, 12, 16, 20], shape: [2, 3, 1], stride: [3, 1, 1] })
    expect(result.h).toEqual({ data: [12, 14, 16, 18, 20, 22], shape: [3, 2], stride: [2, 1] })
    expect(result.i).toEqual({ data: [4, 5, 6, 7, 16, 17, 18, 19], shape: [2, 4], stride: [4, 1] })
    expect(result.j).toEqual({ data: [20, 21, 22, 23], shape: [4], stride: [1] })
  })

  // -------------------------------------------------------------------------
  // Results match between default queue and WorkerPool queue
  // -------------------------------------------------------------------------

  test('WorkerPool queue produces identical results to default queue', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, zarrSlice,
              workerPoolGetOptions, workerPoolSetOptions } = window

      // Setup two identical arrays
      const store1 = new Map<string, Uint8Array>()
      const store2 = new Map<string, Uint8Array>()

      const arr1 = new ZarrArray({
        shape: [4, 6],
        chunk_shape: [2, 3],
        data_type: 'float32' as const,
        store: store1,
      })
      const arr2 = new ZarrArray({
        shape: [4, 6],
        chunk_shape: [2, 3],
        data_type: 'float32' as const,
        store: store2,
      })

      const pool = new WorkerPool(2)

      const data = {
        data: new Float32Array(Array.from({ length: 24 }, (_, i) => i * 1.5)),
        shape: [4, 6],
        stride: [6, 1],
      }

      // Write with default queue
      await zarrSet(arr1, null, data)
      // Write with WorkerPool queue
      await zarrSet(arr2, null, data, workerPoolSetOptions(pool, arr2))

      // Read with both
      const res1 = await zarrGet(arr1, [zarrSlice(1, 3), zarrSlice(2, 5)]) as { data: Float32Array; shape: number[] }
      const res2 = await zarrGet(arr2, [zarrSlice(1, 3), zarrSlice(2, 5)], workerPoolGetOptions(pool, arr2)) as { data: Float32Array; shape: number[] }

      pool.terminateWorkers()

      return {
        default: { data: Array.from(res1.data), shape: res1.shape },
        pool: { data: Array.from(res2.data), shape: res2.shape },
      }
    })

    expect(result.pool.data).toEqual(result.default.data)
    expect(result.pool.shape).toEqual(result.default.shape)
  })

  // -------------------------------------------------------------------------
  // Worker recycling across get/set operations
  // -------------------------------------------------------------------------

  test('workers are recycled across get/set operations', async ({ page }) => {
    const workerCount = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, workerPoolGetOptions, workerPoolSetOptions } = window

      let workersCreated = 0
      const OriginalWorker = Worker
      // Monkey-patch Worker constructor to count creations
      ;(globalThis as any).Worker = class extends OriginalWorker {
        constructor(url: string | URL, opts?: WorkerOptions) {
          super(url, opts)
          workersCreated++
        }
      }

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [4, 6],
        chunk_shape: [2, 3],
        data_type: 'int32' as const,
      })

      const data = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [4, 6],
        stride: [6, 1],
      }

      // Write - this creates workers
      await zarrSet(arr, null, data, workerPoolSetOptions(pool, arr))
      // Read - should reuse workers
      await zarrGet(arr, null, workerPoolGetOptions(pool, arr))
      // Write again - should reuse workers
      await zarrSet(arr, null, data, workerPoolSetOptions(pool, arr))
      // Read again - should reuse workers
      await zarrGet(arr, null, workerPoolGetOptions(pool, arr))

      pool.terminateWorkers()
      ;(globalThis as any).Worker = OriginalWorker

      return workersCreated
    })

    // Pool size is 2, so only 2 workers should ever be created
    expect(workerCount).toBe(2)
  })

  // -------------------------------------------------------------------------
  // Progress callback with WorkerPool
  // -------------------------------------------------------------------------

  test('runTasks with progress for zarrita chunk operations', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })

      // [2,3,4] with chunk [1,2,2] = 2*2*2 = 8 chunks
      // Write data — this will go through 8 chunks
      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }
      await zarrSet(arr, null, DATA, workerPoolSetOptions(pool, arr))

      // Now read using runTasks with progress
      const codecWorkerUrl = new URL('../app/zarrita/codec-worker.ts', window.location.href).href

      const progressUpdates: Array<[number, number]> = []

      // Build task functions for each chunk
      const tasks = []
      for (let d0 = 0; d0 < 2; d0++) {
        for (let d1 = 0; d1 < 2; d1++) {
          for (let d2 = 0; d2 < 2; d2++) {
            const coords = [d0, d1, d2]
            tasks.push((worker: Worker | null) => {
              const w = worker ?? new Worker(codecWorkerUrl, { type: 'module' })
              const key = arr.resolve(arr.encodeChunkKey(coords)).path
              const bytes = arr.store.get(key)
              if (!bytes) {
                return Promise.resolve({ worker: w, result: null })
              }
              return new Promise<{ worker: Worker; result: any }>((resolve) => {
                const id = Math.random()
                const handler = (event: MessageEvent) => {
                  if (event.data.id === id) {
                    w.removeEventListener('message', handler)
                    resolve({ worker: w, result: event.data })
                  }
                }
                w.addEventListener('message', handler)
                const bytesCopy = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
                w.postMessage(
                  { type: 'decode', id, bytes: bytesCopy, dtype: 'int32', shape: [1, 2, 2] },
                  [bytesCopy],
                )
              })
            })
          }
        }
      }

      const { promise } = pool.runTasks(tasks, (completed: number, total: number) => {
        progressUpdates.push([completed, total])
      })
      await promise

      pool.terminateWorkers()

      return {
        totalUpdates: progressUpdates.length,
        totalChunks: 8,
        lastUpdate: progressUpdates[progressUpdates.length - 1],
      }
    })

    expect(result.totalUpdates).toBe(result.totalChunks)
    expect(result.lastUpdate[0]).toBe(8) // all completed
    expect(result.lastUpdate[1]).toBe(8) // total
  })

  // -------------------------------------------------------------------------
  // Multiple sequential onIdle cycles
  // -------------------------------------------------------------------------

  test('multiple sequential get/set/onIdle cycles', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, zarrSlice,
              workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [4, 4],
        chunk_shape: [2, 2],
        data_type: 'int32' as const,
      })

      // Cycle 1: write zeros explicitly
      await zarrSet(arr, null, 0, workerPoolSetOptions(pool, arr))
      let res = await zarrGet(arr, null, workerPoolGetOptions(pool, arr)) as { data: Int32Array }
      const cycle1 = Array.from(res.data)

      // Cycle 2: write 42 to row 0
      await zarrSet(arr, [0, null], 42, workerPoolSetOptions(pool, arr))
      res = await zarrGet(arr, null, workerPoolGetOptions(pool, arr)) as { data: Int32Array }
      const cycle2 = Array.from(res.data)

      // Cycle 3: write sequential data
      const data = {
        data: new Int32Array(Array.from({ length: 16 }, (_, i) => i * 10)),
        shape: [4, 4],
        stride: [4, 1],
      }
      await zarrSet(arr, null, data, workerPoolSetOptions(pool, arr))
      res = await zarrGet(arr, [zarrSlice(1, 3), zarrSlice(1, 3)], workerPoolGetOptions(pool, arr)) as { data: Int32Array; shape: number[] }
      const cycle3 = { data: Array.from(res.data), shape: res.shape }

      pool.terminateWorkers()

      return { cycle1, cycle2, cycle3 }
    })

    expect(result.cycle1).toEqual(new Array(16).fill(0))

    const expectedCycle2 = new Array(16).fill(0)
    expectedCycle2.fill(42, 0, 4)
    expect(result.cycle2).toEqual(expectedCycle2)

    expect(result.cycle3.shape).toEqual([2, 2])
    expect(result.cycle3.data).toEqual([50, 60, 90, 100])
  })

  // -------------------------------------------------------------------------
  // Concurrency with codec delay
  // -------------------------------------------------------------------------

  test('pool of 2 handles 8 chunks with codec delay', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet,
              workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [2, 3, 4],
        chunk_shape: [1, 2, 2],
        data_type: 'int32' as const,
      })

      // 8 chunks total, pool size 2 → must queue
      const DATA = {
        data: new Int32Array(Array.from({ length: 24 }, (_, i) => i)),
        shape: [2, 3, 4],
        stride: [12, 4, 1],
      }

      // Write without delay (fast)
      await zarrSet(arr, null, DATA, workerPoolSetOptions(pool, arr))

      // Read with codec delay to exercise queuing
      const res = await zarrGet(arr, null, workerPoolGetOptions(pool, arr, { codecDelay: 10 })) as {
        data: Int32Array; shape: number[]; stride: number[]
      }

      pool.terminateWorkers()

      return {
        data: Array.from(res.data),
        shape: res.shape,
        stride: res.stride,
      }
    })

    expect(result.data).toEqual(Array.from({ length: 24 }, (_, i) => i))
    expect(result.shape).toEqual([2, 3, 4])
    expect(result.stride).toEqual([12, 4, 1])
  })

  // -------------------------------------------------------------------------
  // Different data types
  // -------------------------------------------------------------------------

  test('works with float64 data type', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet,
              workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [4],
        chunk_shape: [2],
        data_type: 'float64' as const,
      })

      const data = {
        data: new Float64Array([1.5, 2.5, 3.5, 4.5]),
        shape: [4],
        stride: [1],
      }
      await zarrSet(arr, null, data, workerPoolSetOptions(pool, arr))

      const res = await zarrGet(arr, null, workerPoolGetOptions(pool, arr)) as { data: Float64Array }
      pool.terminateWorkers()
      return Array.from(res.data)
    })

    expect(result).toEqual([1.5, 2.5, 3.5, 4.5])
  })

  test('works with uint8 data type', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet,
              workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [4],
        chunk_shape: [2],
        data_type: 'uint8' as const,
      })

      const data = {
        data: new Uint8Array([255, 0, 128, 64]),
        shape: [4],
        stride: [1],
      }
      await zarrSet(arr, null, data, workerPoolSetOptions(pool, arr))

      const res = await zarrGet(arr, null, workerPoolGetOptions(pool, arr)) as { data: Uint8Array }
      pool.terminateWorkers()
      return Array.from(res.data)
    })

    expect(result).toEqual([255, 0, 128, 64])
  })

  // -------------------------------------------------------------------------
  // Fill value handling
  // -------------------------------------------------------------------------

  test('fill value is used for unwritten chunks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const { WorkerPool, ZarrArray, zarrGet, zarrSet, zarrSlice,
              workerPoolGetOptions, workerPoolSetOptions } = window

      const pool = new WorkerPool(2)
      const arr = new ZarrArray({
        shape: [6],
        chunk_shape: [2],
        data_type: 'int32' as const,
        fill_value: -1,
      })

      // Only write to first chunk
      await zarrSet(arr, [zarrSlice(2)], 42, workerPoolSetOptions(pool, arr))

      // Read entire array — unwritten chunks should have fill value
      const res = await zarrGet(arr, null, workerPoolGetOptions(pool, arr)) as { data: Int32Array }
      pool.terminateWorkers()
      return Array.from(res.data)
    })

    expect(result).toEqual([42, 42, -1, -1, -1, -1])
  })
})
