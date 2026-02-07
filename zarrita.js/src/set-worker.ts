/**
 * setWorker() — Worker-accelerated set for zarrita arrays.
 *
 * Writes data to a zarrita Array, offloading codec encode (and decode for
 * partial chunk updates) operations to a WorkerPool. The main thread handles
 * data modification and store writes, while workers handle the expensive
 * codec operations.
 *
 * Uses p-queue for concurrency control over chunk operations.
 */

import PQueue from 'p-queue'
import type { WorkerPool } from '@fideus-labs/worker-pool'
import type {
  Array as ZarrArray,
  Chunk,
  DataType,
  Indices,
  Mutable,
  Projection,
  Scalar,
  Slice,
  TypedArray,
} from 'zarrita'
import { BasicIndexer, type IndexerProjection } from './internals/indexer.js'
import { setter } from './internals/setter.js'
import { get_ctr, get_strides, create_chunk_key_encoder } from './internals/util.js'
import type { SetWorkerOptions, CodecChunkMeta } from './types.js'
import { workerDecode, workerEncode } from './worker-rpc.js'

/**
 * Default URL for the codec worker.
 */
const DEFAULT_WORKER_URL = new URL('./codec-worker.js', import.meta.url)

/**
 * Acquire a worker from the pool's workerQueue.
 */
function acquireWorker(
  pool: WorkerPool,
  workerUrl: string | URL,
): Worker {
  const slot = pool.workerQueue.pop()
  if (slot != null) return slot
  return new Worker(workerUrl, { type: 'module' })
}

/**
 * Return a worker to the pool for reuse.
 */
function releaseWorker(pool: WorkerPool, worker: Worker): void {
  pool.workerQueue.push(worker)
}

/**
 * Read the codec metadata from the zarr store.
 */
async function readCodecMeta<D extends DataType>(
  arr: ZarrArray<D, Mutable>,
): Promise<CodecChunkMeta> {
  const store = arr.store

  // Try v3
  const v3Path = (arr.path === '/' ? '/zarr.json' : `${arr.path}/zarr.json`) as `/${string}`
  const v3Bytes = await store.get(v3Path)
  if (v3Bytes) {
    const metadata = JSON.parse(new TextDecoder().decode(v3Bytes))
    return {
      data_type: metadata.data_type,
      chunk_shape: metadata.chunk_grid.configuration.chunk_shape,
      codecs: metadata.codecs,
    }
  }

  // Try v2
  const v2Path = (arr.path === '/' ? '/.zarray' : `${arr.path}/.zarray`) as `/${string}`
  const v2Bytes = await store.get(v2Path)
  if (v2Bytes) {
    const metadata = JSON.parse(new TextDecoder().decode(v2Bytes))
    const codecs: Array<{ name: string; configuration: Record<string, unknown> }> = []
    if (metadata.order === 'F') {
      codecs.push({ name: 'transpose', configuration: { order: 'F' } })
    }
    if (metadata.compressor) {
      const { id, ...configuration } = metadata.compressor
      codecs.push({ name: id, configuration })
    }
    for (const { id, ...configuration } of metadata.filters ?? []) {
      codecs.push({ name: id, configuration })
    }
    return {
      data_type: arr.dtype,
      chunk_shape: arr.chunks,
      codecs: codecs.length > 0 ? codecs : [{ name: 'bytes', configuration: { endian: 'little' } }],
    }
  }

  // Fallback
  return {
    data_type: arr.dtype,
    chunk_shape: arr.chunks,
    codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
  }
}

/**
 * Read the chunk key encoding from store metadata.
 */
async function readChunkKeyEncoding<D extends DataType>(
  arr: ZarrArray<D, Mutable>,
): Promise<(chunk_coords: number[]) => string> {
  const store = arr.store

  const v3Path = (arr.path === '/' ? '/zarr.json' : `${arr.path}/zarr.json`) as `/${string}`
  const v3Bytes = await store.get(v3Path)
  if (v3Bytes) {
    const metadata = JSON.parse(new TextDecoder().decode(v3Bytes))
    return create_chunk_key_encoder(metadata.chunk_key_encoding)
  }

  const v2Path = (arr.path === '/' ? '/.zarray' : `${arr.path}/.zarray`) as `/${string}`
  const v2Bytes = await store.get(v2Path)
  if (v2Bytes) {
    const metadata = JSON.parse(new TextDecoder().decode(v2Bytes))
    return create_chunk_key_encoder({
      name: 'v2',
      configuration: { separator: metadata.dimension_separator ?? '.' },
    })
  }

  return create_chunk_key_encoder({ name: 'default' })
}

function flip_indexer_projection(
  m: IndexerProjection,
): { from: number | Indices | null; to: number | Indices | null } {
  if (m.to == null) return { from: m.to, to: m.from }
  return { from: m.to, to: m.from }
}

function is_total_slice(
  selection: (number | Indices)[],
  shape: readonly number[],
): boolean {
  return selection.every((s, i) => {
    if (typeof s === 'number') return false
    const [start, stop, step] = s
    return stop - start === shape[i] && step === 1
  })
}

/**
 * Write data to a zarrita Array with codec encode/decode offloaded to Web Workers.
 *
 * Drop-in replacement for zarrita's `set()` with worker acceleration.
 * Workers handle codec encoding (and decoding for partial chunk updates)
 * while the main thread handles data modification and store writes.
 *
 * @param arr       - The zarrita Array to write to.
 * @param selection - Index selection (null for full array, or per-dimension slices/indices).
 * @param value     - Scalar value or Chunk to write.
 * @param opts      - Options including the WorkerPool and concurrency.
 *
 * @example
 * ```ts
 * import { WorkerPool } from '@fideus-labs/worker-pool'
 * import { setWorker } from '@fideus-labs/zarrita.js'
 * import * as zarr from 'zarrita'
 *
 * const pool = new WorkerPool(4)
 * const store = zarr.root(new Map())
 * const arr = await zarr.create(store, {
 *   shape: [100, 100],
 *   chunk_shape: [10, 10],
 *   data_type: 'float32',
 * })
 * await setWorker(arr, null, 42.0, { pool })
 *
 * pool.terminateWorkers()
 * ```
 */
export async function setWorker<D extends DataType>(
  arr: ZarrArray<D, Mutable>,
  selection: (number | Slice | null)[] | null,
  value: Scalar<D> | Chunk<D>,
  opts: SetWorkerOptions,
): Promise<void> {
  const { pool, concurrency, workerUrl } = opts
  const resolvedWorkerUrl = workerUrl ?? DEFAULT_WORKER_URL

  // Read metadata from store
  const [codecMeta, encodeChunkKey] = await Promise.all([
    readCodecMeta(arr),
    readChunkKeyEncoding(arr),
  ])

  const Ctr = get_ctr(arr.dtype)

  // Set up the indexer
  const indexer = new BasicIndexer({
    selection,
    shape: arr.shape,
    chunk_shape: arr.chunks,
  })

  const chunkSize = arr.chunks.reduce((a: number, b: number) => a * b, 1)

  // Read fill value from metadata
  let fillValue: Scalar<D> | null = null
  {
    const v3Path = (arr.path === '/' ? '/zarr.json' : `${arr.path}/zarr.json`) as `/${string}`
    const v3Bytes = await arr.store.get(v3Path)
    if (v3Bytes) {
      const metadata = JSON.parse(new TextDecoder().decode(v3Bytes))
      fillValue = metadata.fill_value
    }
  }

  // Create p-queue for concurrency control
  const poolSize = Math.max(pool.workerQueue.length, 1)
  const queue = new PQueue({ concurrency: concurrency ?? poolSize })

  for (const { chunk_coords, mapping } of indexer) {
    const chunkSelection = mapping.map((m) => m.from)
    const flipped = mapping.map(flip_indexer_projection)

    void queue.add(async () => {
      const chunkKey = encodeChunkKey(chunk_coords)
      const chunkPath = arr.resolve(chunkKey).path
      const chunkShape = arr.chunks.slice()
      const chunkStride = get_strides(chunkShape)

      let chunkData: TypedArray<D>

      if (is_total_slice(chunkSelection, chunkShape)) {
        // Totally replace this chunk — no need to fetch existing data
        chunkData = new Ctr(chunkSize) as TypedArray<D>
        if (typeof value === 'object' && value !== null) {
          const chunk = setter.prepare(chunkData, chunkShape.slice(), chunkStride.slice()) as Chunk<D>
          setter.set_from_chunk(
            chunk,
            value as Chunk<D>,
            flipped as Projection[],
          )
        } else {
          // @ts-expect-error: scalar fill
          chunkData.fill(value)
        }
      } else {
        // Partial replacement — fetch and decode existing chunk first
        const rawBytes = await arr.store.get(chunkPath as `/${string}`)

        if (rawBytes) {
          // Decode existing chunk on worker
          const worker = acquireWorker(pool, resolvedWorkerUrl)
          try {
            const decoded = await workerDecode<D>(worker, rawBytes, codecMeta)
            chunkData = decoded.data
            releaseWorker(pool, worker)
          } catch (error) {
            worker.terminate()
            throw error
          }
        } else {
          // Missing chunk — start from fill value
          chunkData = new Ctr(chunkSize) as TypedArray<D>
          if (fillValue != null) {
            // @ts-expect-error: fill_value union
            chunkData.fill(fillValue)
          }
        }

        const chunk = setter.prepare(chunkData, chunkShape.slice(), chunkStride.slice()) as Chunk<D>
        if (typeof value === 'object' && value !== null) {
          setter.set_from_chunk(
            chunk,
            value as Chunk<D>,
            flipped as Projection[],
          )
        } else {
          setter.set_scalar(chunk, chunkSelection as (number | Indices)[], value as Scalar<D>)
        }
      }

      // Encode the chunk on a worker
      const worker = acquireWorker(pool, resolvedWorkerUrl)
      try {
        const encoded = await workerEncode<D>(worker, chunkData, codecMeta)
        releaseWorker(pool, worker)

        // Write to store on main thread
        await arr.store.set(chunkPath as `/${string}`, encoded)
      } catch (error) {
        worker.terminate()
        throw error
      }
    })
  }

  await queue.onIdle()
}
