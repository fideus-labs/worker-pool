/**
 * Core benchmark engine.
 *
 * Runs timed iterations of vanilla zarrita get/set vs worker-pool
 * getWorker/setWorker, collecting timing statistics.
 */

import * as zarr from 'zarrita'
import type { DataType, Chunk, Array as ZarrArray, Readable, Mutable } from 'zarrita'
import { FetchStore } from '@zarrita/storage'
import { WorkerPool } from '@fideus-labs/worker-pool'
import { getWorker, setWorker } from '@fideus-labs/fizarrita'
import { createSyntheticArray } from './synthetic.js'
import codecWorkerUrl from '$root/fizarrita/src/codec-worker.ts?worker&url'
import type {
  BenchmarkConfig,
  BenchmarkResult,
  BenchmarkOperation,
  OperationResult,
  TimingStats,
  ProgressCallback,
  BenchmarkProgress,
  SyntheticConfig,
  RemoteConfig,
} from './types.js'

// ---------------------------------------------------------------------------
// Stats helpers
// ---------------------------------------------------------------------------

function computeStats(raw: number[]): TimingStats {
  const sorted = [...raw].sort((a, b) => a - b)
  const n = sorted.length
  const mean = raw.reduce((a, b) => a + b, 0) / n
  const median =
    n % 2 === 0
      ? (sorted[n / 2 - 1] + sorted[n / 2]) / 2
      : sorted[Math.floor(n / 2)]
  const min = sorted[0]
  const max = sorted[n - 1]
  const variance = raw.reduce((s, v) => s + (v - mean) ** 2, 0) / n
  const stddev = Math.sqrt(variance)
  return { mean, median, min, max, stddev, raw }
}

// ---------------------------------------------------------------------------
// Abort support
// ---------------------------------------------------------------------------

let abortController: AbortController | null = null

export function cancelBenchmark(): void {
  abortController?.abort()
}

function checkAbort(): void {
  if (abortController?.signal.aborted) {
    throw new DOMException('Benchmark cancelled', 'AbortError')
  }
}

// ---------------------------------------------------------------------------
// Open remote array
// ---------------------------------------------------------------------------

async function openRemoteArray(
  config: RemoteConfig,
): Promise<ZarrArray<DataType, Readable>> {
  const store = new FetchStore(config.rootUrl)
  const location = zarr.root(store)
  const arr = await zarr.open(location.resolve(config.arrayPath), {
    kind: 'array',
  })
  return arr as ZarrArray<DataType, Readable>
}

// ---------------------------------------------------------------------------
// Timing helper
// ---------------------------------------------------------------------------

async function timeAsync(fn: () => Promise<void>): Promise<number> {
  const start = performance.now()
  await fn()
  return performance.now() - start
}

// ---------------------------------------------------------------------------
// Main benchmark runner
// ---------------------------------------------------------------------------

export async function runBenchmark(
  config: BenchmarkConfig,
  onProgress?: ProgressCallback,
): Promise<BenchmarkResult> {
  abortController = new AbortController()

  const results: OperationResult[] = []

  // Determine total steps for progress tracking
  const totalOps = config.operations.length
  let variantsPerOp = 2 // vanilla + worker
  if (config.useSharedArrayBuffer) variantsPerOp++
  if (config.useChunkCache) variantsPerOp++
  const totalStepsPerOp = (config.warmupRuns + config.iterations) * variantsPerOp
  const totalSteps = totalOps * totalStepsPerOp
  let currentStep = 0

  function report(
    phase: BenchmarkProgress['phase'],
    operation: BenchmarkOperation,
    variant: BenchmarkProgress['variant'],
    iteration: number,
    totalIterations: number,
  ): void {
    currentStep++
    onProgress?.({
      phase,
      operation,
      variant,
      iteration,
      totalIterations,
      progress: currentStep / totalSteps,
    })
  }

  // ---- Set up data sources ---- //

  let readArr: ZarrArray<DataType, Readable>
  let writeArr: ZarrArray<DataType, Readable & Mutable> | null = null
  let writeRefData: ArrayBufferView | null = null
  let writeStrides: number[] | null = null

  if (config.dataset.source === 'synthetic') {
    const synConfig = config.dataset as SyntheticConfig
    const { arr, refData } = await createSyntheticArray(synConfig)
    readArr = arr as unknown as ZarrArray<DataType, Readable>
    if (config.operations.includes('write')) {
      writeArr = arr
      writeRefData = refData
      writeStrides = new Array<number>(synConfig.shape.length)
      writeStrides[synConfig.shape.length - 1] = 1
      for (let i = synConfig.shape.length - 2; i >= 0; i--) {
        writeStrides[i] = writeStrides[i + 1] * synConfig.shape[i + 1]
      }
    }
  } else {
    const remoteConfig = config.dataset as RemoteConfig
    readArr = await openRemoteArray(remoteConfig)
  }

  // ---- Create worker pool ---- //

  const pool = new WorkerPool(config.poolSize)
  const workerUrl = codecWorkerUrl

  // ---- Run benchmarks for each operation ---- //

  for (const op of config.operations) {
    checkAbort()

    if (op === 'read') {
      // --- Vanilla read ---
      const vanillaTimes: number[] = []

      // Warmup
      for (let i = 0; i < config.warmupRuns; i++) {
        checkAbort()
        await zarr.get(readArr, null)
        report('warmup', 'read', 'vanilla', i + 1, config.warmupRuns)
      }

      // Timed
      for (let i = 0; i < config.iterations; i++) {
        checkAbort()
        const ms = await timeAsync(() => zarr.get(readArr, null).then(() => {}))
        vanillaTimes.push(ms)
        report('benchmark', 'read', 'vanilla', i + 1, config.iterations)
      }

      results.push({
        operation: 'read',
        variant: 'vanilla',
        stats: computeStats(vanillaTimes),
      })

      // --- Worker read ---
      const workerTimes: number[] = []

      // Warmup
      for (let i = 0; i < config.warmupRuns; i++) {
        checkAbort()
        await getWorker(readArr, null, { pool, workerUrl })
        report('warmup', 'read', 'worker', i + 1, config.warmupRuns)
      }

      // Timed
      for (let i = 0; i < config.iterations; i++) {
        checkAbort()
        const ms = await timeAsync(() =>
          getWorker(readArr, null, { pool, workerUrl }).then(() => {}),
        )
        workerTimes.push(ms)
        report('benchmark', 'read', 'worker', i + 1, config.iterations)
      }

      results.push({
        operation: 'read',
        variant: 'worker',
        stats: computeStats(workerTimes),
      })

      // --- Worker + SharedArrayBuffer read ---
      if (config.useSharedArrayBuffer) {
        const sabTimes: number[] = []

        // Warmup
        for (let i = 0; i < config.warmupRuns; i++) {
          checkAbort()
          await getWorker(readArr, null, { pool, workerUrl, useSharedArrayBuffer: true })
          report('warmup', 'read', 'worker-sab', i + 1, config.warmupRuns)
        }

        // Timed
        for (let i = 0; i < config.iterations; i++) {
          checkAbort()
          const ms = await timeAsync(() =>
            getWorker(readArr, null, { pool, workerUrl, useSharedArrayBuffer: true }).then(() => {}),
          )
          sabTimes.push(ms)
          report('benchmark', 'read', 'worker-sab', i + 1, config.iterations)
        }

        results.push({
          operation: 'read',
          variant: 'worker-sab',
          stats: computeStats(sabTimes),
        })
      }

      // --- Worker + Chunk Cache read ---
      if (config.useChunkCache) {
        const cacheTimes: number[] = []
        const cache = new Map()

        // Warmup — these iterations populate the cache
        for (let i = 0; i < config.warmupRuns; i++) {
          checkAbort()
          await getWorker(readArr, null, { pool, workerUrl, cache })
          report('warmup', 'read', 'worker-cache', i + 1, config.warmupRuns)
        }

        // Timed — all iterations hit a fully-populated cache
        for (let i = 0; i < config.iterations; i++) {
          checkAbort()
          const ms = await timeAsync(() =>
            getWorker(readArr, null, { pool, workerUrl, cache }).then(() => {}),
          )
          cacheTimes.push(ms)
          report('benchmark', 'read', 'worker-cache', i + 1, config.iterations)
        }

        results.push({
          operation: 'read',
          variant: 'worker-cache',
          stats: computeStats(cacheTimes),
        })
      }
    }

    if (op === 'write' && writeArr && writeRefData && writeStrides) {
      const shape = (config.dataset as SyntheticConfig).shape

      // --- Vanilla write ---
      const vanillaTimes: number[] = []

      // Warmup
      for (let i = 0; i < config.warmupRuns; i++) {
        checkAbort()
        await zarr.set(writeArr, null, {
          data: writeRefData,
          shape: shape.slice(),
          stride: writeStrides.slice(),
        } as Chunk<DataType>)
        report('warmup', 'write', 'vanilla', i + 1, config.warmupRuns)
      }

      // Timed
      for (let i = 0; i < config.iterations; i++) {
        checkAbort()
        const ms = await timeAsync(() =>
          zarr.set(writeArr!, null, {
            data: writeRefData!,
            shape: shape.slice(),
            stride: writeStrides!.slice(),
          } as Chunk<DataType>),
        )
        vanillaTimes.push(ms)
        report('benchmark', 'write', 'vanilla', i + 1, config.iterations)
      }

      results.push({
        operation: 'write',
        variant: 'vanilla',
        stats: computeStats(vanillaTimes),
      })

      // --- Worker write ---
      const workerTimes: number[] = []

      // Warmup
      for (let i = 0; i < config.warmupRuns; i++) {
        checkAbort()
        await setWorker(writeArr, null, {
          data: writeRefData,
          shape: shape.slice(),
          stride: writeStrides.slice(),
        } as Chunk<DataType>, { pool, workerUrl })
        report('warmup', 'write', 'worker', i + 1, config.warmupRuns)
      }

      // Timed
      for (let i = 0; i < config.iterations; i++) {
        checkAbort()
        const ms = await timeAsync(() =>
          setWorker(writeArr!, null, {
            data: writeRefData!,
            shape: shape.slice(),
            stride: writeStrides!.slice(),
          } as Chunk<DataType>, { pool, workerUrl }),
        )
        workerTimes.push(ms)
        report('benchmark', 'write', 'worker', i + 1, config.iterations)
      }

      results.push({
        operation: 'write',
        variant: 'worker',
        stats: computeStats(workerTimes),
      })
    }
  }

  // ---- Cleanup ---- //
  pool.terminateWorkers()
  abortController = null

  return {
    config,
    results,
    timestamp: Date.now(),
  }
}
