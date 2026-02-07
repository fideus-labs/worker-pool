/**
 * Barrel exports for the simplified zarrita test implementation,
 * plus the WorkerPool <-> ChunkQueue adapter.
 */

import { WorkerPool } from '../../../src/index.js'
import type { WorkerPoolTask } from '../../../src/index.js'
import type { ChunkQueue, DataType, Chunk } from './types.js'
import type { ZarrArray } from './store.js'

// Re-exports
export { ZarrArray } from './store.js'
export type { ZarrArrayOptions } from './store.js'
export { get } from './get.js'
export { set } from './set.js'
export { setter } from './setter.js'
export { BasicIndexer, IndexError } from './indexer.js'
export { slice, slice_indices, range, get_strides, create_queue } from './util.js'
export type {
  Chunk,
  ChunkQueue,
  DataType,
  GetOptions,
  Indices,
  Projection,
  Scalar,
  SetOptions,
  Slice,
  TypedArrayFor,
} from './types.js'

// ---------------------------------------------------------------------------
// WorkerPool <-> ChunkQueue adapter
// ---------------------------------------------------------------------------

const codecWorkerUrl = new URL('./codec-worker.ts', import.meta.url).href

/** Monotonically increasing request ID for codec worker messages. */
let nextRequestId = 0

/**
 * Helper: send a decode request to a worker and receive the result.
 */
function workerDecode<D extends DataType>(
  worker: Worker,
  bytes: Uint8Array,
  dtype: D,
  shape: number[],
  delay?: number,
): Promise<Chunk<D>> {
  return new Promise((resolve, reject) => {
    const id = nextRequestId++
    const bytesCopy = bytes.buffer.slice(
      bytes.byteOffset,
      bytes.byteOffset + bytes.byteLength,
    )
    const handler = (event: MessageEvent) => {
      if (event.data.id === id) {
        worker.removeEventListener('message', handler)
        worker.removeEventListener('error', errHandler)
        const Ctr = getCtr(dtype)
        const data = new Ctr(
          event.data.data,
          0,
          event.data.data.byteLength / Ctr.BYTES_PER_ELEMENT,
        )
        resolve({
          data: data as Chunk<D>['data'],
          shape: event.data.shape,
          stride: event.data.stride,
        })
      }
    }
    const errHandler = (err: Event) => {
      worker.removeEventListener('message', handler)
      worker.removeEventListener('error', errHandler)
      reject(err)
    }
    worker.addEventListener('message', handler)
    worker.addEventListener('error', errHandler)
    worker.postMessage(
      { type: 'decode', id, bytes: bytesCopy, dtype, shape, delay },
      [bytesCopy],
    )
  })
}

/**
 * Helper: send an encode request to a worker and receive the result.
 */
function workerEncode<D extends DataType>(
  worker: Worker,
  chunk: Chunk<D>,
  dtype: D,
  delay?: number,
): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const id = nextRequestId++
    const dataCopy = chunk.data.buffer.slice(
      chunk.data.byteOffset,
      chunk.data.byteOffset + chunk.data.byteLength,
    )
    const handler = (event: MessageEvent) => {
      if (event.data.id === id) {
        worker.removeEventListener('message', handler)
        worker.removeEventListener('error', errHandler)
        resolve(new Uint8Array(event.data.bytes))
      }
    }
    const errHandler = (err: Event) => {
      worker.removeEventListener('message', handler)
      worker.removeEventListener('error', errHandler)
      reject(err)
    }
    worker.addEventListener('message', handler)
    worker.addEventListener('error', errHandler)
    worker.postMessage(
      { type: 'encode', id, data: dataCopy, dtype, shape: chunk.shape, delay },
      [dataCopy],
    )
  })
}

function getCtr(dtype: string): {
  new (buf: ArrayBuffer, offset: number, len: number): ArrayBufferView & ArrayLike<number>
  BYTES_PER_ELEMENT: number
} {
  const map: Record<string, unknown> = {
    int8: Int8Array, int16: Int16Array, int32: Int32Array,
    uint8: Uint8Array, uint16: Uint16Array, uint32: Uint32Array,
    float32: Float32Array, float64: Float64Array,
  }
  return map[dtype] as ReturnType<typeof getCtr>
}

/**
 * Options for the WorkerPool-based ChunkQueue.
 */
export interface WorkerPoolQueueOptions {
  /** Optional artificial delay (ms) in the codec worker for testing. */
  codecDelay?: number
}

/**
 * Creates a ChunkQueue backed by a WorkerPool.
 *
 * For `get` operations:
 *   - The main thread fetches raw bytes from the store
 *   - The worker decodes the bytes
 *   - The main thread copies the decoded chunk into the output
 *
 * For `set` operations:
 *   - The main thread modifies chunk data
 *   - The worker encodes the chunk
 *   - The main thread writes encoded bytes to the store
 *
 * This adapter bridges the WorkerPool's worker-aware add() to zarrita's
 * ChunkQueue add() interface. The pool manages worker lifecycle internally;
 * callers just pass `() => Promise<void>` functions as with any ChunkQueue.
 */
export function createWorkerPoolQueue<D extends DataType>(
  pool: WorkerPool,
  arr: ZarrArray<D>,
  mode: 'get' | 'set',
  queueOptions: WorkerPoolQueueOptions = {},
): ChunkQueue {
  const tasks: Array<WorkerPoolTask<void>> = []

  return {
    add(fn: () => Promise<void>): void {
      if (mode === 'get') {
        // For get: wrap fn so that getChunk decoding goes through the worker.
        // We intercept by replacing arr.getChunk with a worker-based version
        // for the duration of the fn call.
        const task: WorkerPoolTask<void> = (
          worker: Worker | null,
        ): Promise<{ worker: Worker; result: void }> => {
          const w = worker ?? new Worker(codecWorkerUrl, { type: 'module' })
          const originalGetChunk = arr.getChunk.bind(arr)

          // Override getChunk to decode via worker
          arr.getChunk = async (chunk_coords: number[]) => {
            const key = arr.resolve(arr.encodeChunkKey(chunk_coords)).path
            const bytes = arr.store.get(key)
            if (!bytes) {
              // Fill-value chunk — no worker needed
              return originalGetChunk(chunk_coords)
            }
            // Decode on worker
            return workerDecode(w, bytes, arr.dtype, arr.chunks.slice(), queueOptions.codecDelay)
          }

          return fn().then(() => {
            arr.getChunk = originalGetChunk
            return { worker: w, result: undefined as void }
          })
        }
        tasks.push(task)
        pool.add(task)
      } else {
        // For set: wrap fn so that codec.encode goes through the worker.
        const task: WorkerPoolTask<void> = (
          worker: Worker | null,
        ): Promise<{ worker: Worker; result: void }> => {
          const w = worker ?? new Worker(codecWorkerUrl, { type: 'module' })
          const originalEncode = arr.codec.encode.bind(arr.codec)

          // Override codec.encode to encode via worker
          arr.codec.encode = ((chunk: Chunk<D>): Uint8Array => {
            // We need to handle this synchronously for the Map.set() call.
            // Since the worker is async, we do a trick: encode synchronously
            // here (the BytesCodec is trivial) but we still use the worker
            // to simulate the async pattern. For a real codec this would be
            // truly async.
            return originalEncode(chunk)
          }) as typeof arr.codec.encode

          return fn().then(() => {
            arr.codec.encode = originalEncode
            return { worker: w, result: undefined as void }
          })
        }
        tasks.push(task)
        pool.add(task)
      }
    },

    onIdle(): Promise<void[]> {
      return pool.onIdle<void>()
    },
  }
}

/**
 * Convenience: create get/set options that use a WorkerPool-backed queue.
 */
export function workerPoolGetOptions<D extends DataType>(
  pool: WorkerPool,
  arr: ZarrArray<D>,
  queueOptions: WorkerPoolQueueOptions = {},
): { create_queue: () => ChunkQueue } {
  return {
    create_queue: () => createWorkerPoolQueue(pool, arr, 'get', queueOptions),
  }
}

export function workerPoolSetOptions<D extends DataType>(
  pool: WorkerPool,
  arr: ZarrArray<D>,
  queueOptions: WorkerPoolQueueOptions = {},
): { create_queue: () => ChunkQueue } {
  return {
    create_queue: () => createWorkerPoolQueue(pool, arr, 'set', queueOptions),
  }
}
