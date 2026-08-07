import { createWorker, isNodeRuntime, WorkerPool } from '../../src/index.js'
import type { WorkerLike, WorkerPoolTask } from '../../src/index.js'

// URL for the test worker — Vite handles bundling via the ?worker&url suffix.
const testWorkerUrl = new URL('../browser/test-worker.ts', import.meta.url).href

/**
 * Helper: run a single task on a worker. This is the canonical task-function
 * shape expected by the pool.
 *
 * It receives a WorkerLike | null (null → create new), posts a message, waits
 * for the response, and returns { worker, result }.
 */
function createSquareTask(
  value: number,
  delay = 0
): WorkerPoolTask<number> {
  return (worker: WorkerLike | null): Promise<{ worker: WorkerLike; result: number }> => {
    const w: WorkerLike = worker ?? new Worker(testWorkerUrl, { type: 'module' })
    return new Promise((resolve, reject) => {
      const onMessage = (event: MessageEvent<{ result: number }>) => {
        detach()
        resolve({ worker: w, result: event.data.result })
      }
      const onError = (err: unknown) => {
        detach()
        reject(err)
      }
      const detach = () => {
        w.removeEventListener('message', onMessage)
        w.removeEventListener('error', onError)
      }
      w.addEventListener('message', onMessage)
      w.addEventListener('error', onError)
      w.postMessage({ value, delay })
    })
  }
}

/**
 * Helper: create a task that always rejects.
 */
function createFailingTask(): WorkerPoolTask<never> {
  return (worker: WorkerLike | null): Promise<{ worker: WorkerLike; result: never }> => {
    const w: WorkerLike = worker ?? new Worker(testWorkerUrl, { type: 'module' })
    return Promise.reject(new Error('intentional failure'))
  }
}

// ---------------------------------------------------------------------------
// Zarrita-compatible helpers
// ---------------------------------------------------------------------------

import {
  ZarrArray,
  get as zarrGet,
  set as zarrSet,
  slice as zarrSlice,
  range as zarrRange,
  IndexError,
  create_queue as createDefaultQueue,
  workerPoolGetOptions,
  workerPoolSetOptions,
} from './zarrita/index.js'

import type {
  Chunk,
  DataType,
  GetOptions,
  SetOptions,
  Slice,
  WorkerPoolQueueOptions,
} from './zarrita/index.js'

// ---------------------------------------------------------------------------
// @fideus-labs/fizarrita — real zarrita integration
// ---------------------------------------------------------------------------

import * as zarr from 'zarrita'
import { getWorker, setWorker, createDefaultWorker, readZstdFrameContentSize, readBloscFrameContentSize, inferChunkShape, hasSizeChangingCodec } from '../../fizarrita/src/index.js'
import type { GetWorkerOptions, SetWorkerOptions, ChunkCache } from '../../fizarrita/src/index.js'

// Expose helpers on the window so Playwright tests can call them.
declare global {
  interface Window {
    WorkerPool: typeof WorkerPool
    createWorker: typeof createWorker
    isNodeRuntime: typeof isNodeRuntime
    createSquareTask: typeof createSquareTask
    createFailingTask: typeof createFailingTask
    testWorkerUrl: string
    // Zarrita helpers (test port)
    ZarrArray: typeof ZarrArray
    zarrGet: typeof zarrGet
    zarrSet: typeof zarrSet
    zarrSlice: typeof zarrSlice
    zarrRange: typeof zarrRange
    IndexError: typeof IndexError
    createDefaultQueue: typeof createDefaultQueue
    workerPoolGetOptions: typeof workerPoolGetOptions
    workerPoolSetOptions: typeof workerPoolSetOptions
    // Real zarrita + getWorker/setWorker
    zarr: typeof zarr
    getWorker: typeof getWorker
    setWorker: typeof setWorker
    createDefaultWorker: typeof createDefaultWorker
    readZstdFrameContentSize: typeof readZstdFrameContentSize
    readBloscFrameContentSize: typeof readBloscFrameContentSize
    inferChunkShape: typeof inferChunkShape
    hasSizeChangingCodec: typeof hasSizeChangingCodec
  }
}

window.WorkerPool = WorkerPool
window.createWorker = createWorker
window.isNodeRuntime = isNodeRuntime
window.createSquareTask = createSquareTask
window.createFailingTask = createFailingTask
window.testWorkerUrl = testWorkerUrl

// Zarrita (test port)
window.ZarrArray = ZarrArray
window.zarrGet = zarrGet
window.zarrSet = zarrSet
window.zarrSlice = zarrSlice
window.zarrRange = zarrRange
window.IndexError = IndexError
window.createDefaultQueue = createDefaultQueue
window.workerPoolGetOptions = workerPoolGetOptions
window.workerPoolSetOptions = workerPoolSetOptions

// Real zarrita + worker-accelerated get/set
window.zarr = zarr
window.getWorker = getWorker
window.setWorker = setWorker
window.createDefaultWorker = createDefaultWorker
window.readZstdFrameContentSize = readZstdFrameContentSize
window.readBloscFrameContentSize = readBloscFrameContentSize
window.inferChunkShape = inferChunkShape
window.hasSizeChangingCodec = hasSizeChangingCodec
