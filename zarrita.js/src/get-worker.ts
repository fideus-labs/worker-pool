/**
 * getWorker() — Worker-accelerated get for zarrita arrays.
 *
 * Reads data from a zarrita Array, offloading codec decode operations to a
 * WorkerPool. The main thread fetches raw bytes from the store, transfers
 * them to a worker for decoding, then copies the decoded chunk into the
 * output array on the main thread.
 *
 * Uses p-queue for concurrency control over chunk operations.
 */

import PQueue from 'p-queue'
import type { WorkerPool } from '@fideus-labs/worker-pool'
import type {
  Array as ZarrArray,
  Chunk,
  DataType,
  Readable,
  Scalar,
  Slice,
} from 'zarrita'
import { BasicIndexer } from './internals/indexer.js'
import { setter } from './internals/setter.js'
import { get_ctr, get_strides, create_chunk_key_encoder } from './internals/util.js'
import type { GetWorkerOptions, CodecChunkMeta } from './types.js'
import { workerDecode } from './worker-rpc.js'

/**
 * Default URL for the codec worker. Uses `import.meta.url` to resolve
 * relative to this module.
 */
const DEFAULT_WORKER_URL = new URL('./codec-worker.js', import.meta.url)

/**
 * Acquire a worker from the pool's workerQueue. If the slot is `null`,
 * creates a new Worker.
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
 * Read the codec metadata from the zarr store for a given array.
 * Supports both v3 (zarr.json) and v2 (.zarray) formats.
 *
 * Falls back to a plain BytesCodec if no metadata is found in the store
 * (e.g., for in-memory arrays created via zarr.create that persist metadata
 * in the map).
 */
async function readCodecMeta<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
): Promise<CodecChunkMeta> {
  const store = arr.store

  // Try v3 first: read zarr.json
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

  // Try v2: read .zarray
  const v2Path = (arr.path === '/' ? '/.zarray' : `${arr.path}/.zarray`) as `/${string}`
  const v2Bytes = await store.get(v2Path)
  if (v2Bytes) {
    const metadata = JSON.parse(new TextDecoder().decode(v2Bytes))
    // Reconstruct v3 codec metadata from v2 format
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

  // Fallback: BytesCodec only
  return {
    data_type: arr.dtype,
    chunk_shape: arr.chunks,
    codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
  }
}

/**
 * Read the chunk key encoding from the zarr store metadata.
 * Falls back to default v3 encoding ("c/0/1/...").
 */
async function readChunkKeyEncoding<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
): Promise<(chunk_coords: number[]) => string> {
  const store = arr.store

  // Try v3
  const v3Path = (arr.path === '/' ? '/zarr.json' : `${arr.path}/zarr.json`) as `/${string}`
  const v3Bytes = await store.get(v3Path)
  if (v3Bytes) {
    const metadata = JSON.parse(new TextDecoder().decode(v3Bytes))
    return create_chunk_key_encoder(metadata.chunk_key_encoding)
  }

  // Try v2
  const v2Path = (arr.path === '/' ? '/.zarray' : `${arr.path}/.zarray`) as `/${string}`
  const v2Bytes = await store.get(v2Path)
  if (v2Bytes) {
    const metadata = JSON.parse(new TextDecoder().decode(v2Bytes))
    return create_chunk_key_encoder({
      name: 'v2',
      configuration: { separator: metadata.dimension_separator ?? '.' },
    })
  }

  // Default v3
  return create_chunk_key_encoder({ name: 'default' })
}

/**
 * Read data from a zarrita Array with codec decoding offloaded to Web Workers.
 *
 * Drop-in replacement for zarrita's `get()` with worker acceleration.
 * The main thread fetches raw bytes from the store, then workers handle
 * the (potentially expensive) codec decode operations in parallel.
 *
 * @param arr       - The zarrita Array to read from.
 * @param selection - Index selection (null for full array, or per-dimension slices/indices).
 * @param opts      - Options including the WorkerPool, concurrency, and store options.
 * @returns The result chunk, or a scalar if all dimensions are integer-indexed.
 *
 * @example
 * ```ts
 * import { WorkerPool } from '@fideus-labs/worker-pool'
 * import { getWorker } from '@fideus-labs/zarrita.js'
 * import * as zarr from 'zarrita'
 *
 * const pool = new WorkerPool(4)
 * const store = new zarr.FetchStore('https://example.com/data.zarr')
 * const arr = await zarr.open(store, { kind: 'array' })
 * const result = await getWorker(arr, null, { pool })
 *
 * pool.terminateWorkers()
 * ```
 */
export async function getWorker<
  D extends DataType,
  Store extends Readable,
  Sel extends (null | Slice | number)[],
>(
  arr: ZarrArray<D, Store>,
  selection: Sel | null = null,
  opts: GetWorkerOptions<Parameters<Store['get']>[1]>,
): Promise<
  null extends Sel[number]
    ? Chunk<D>
    : Slice extends Sel[number]
      ? Chunk<D>
      : Scalar<D>
> {
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

  // Allocate output
  const size = indexer.shape.reduce((a: number, b: number) => a * b, 1)
  const data = new Ctr(size)
  const outStride = get_strides(indexer.shape)
  const out = setter.prepare(data, indexer.shape, outStride) as Chunk<D>

  // Determine fill value from arr (use getChunk for a missing chunk to find it)
  // We'll just use 0 as default since arr doesn't expose fill_value
  // For correctness, fill value chunks just need to be all-zero (or whatever
  // the TypedArray constructor initializes to), which is fine for the fill_value=0 case.
  // For non-zero fill values, we'd need to read metadata. Let's read it.
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

  // Process each chunk
  for (const { chunk_coords, mapping } of indexer) {
    void queue.add(async () => {
      // Fetch raw bytes from store on main thread
      const chunkKey = encodeChunkKey(chunk_coords)
      const chunkPath = arr.resolve(chunkKey).path
      const rawBytes = await arr.store.get(chunkPath, opts.opts)

      let chunk: Chunk<D>

      if (!rawBytes) {
        // Missing chunk — fill value, no worker needed
        const chunkSize = arr.chunks.reduce((a: number, b: number) => a * b, 1)
        const chunkData = new Ctr(chunkSize)
        if (fillValue != null) {
          // @ts-expect-error: fill_value type is union
          chunkData.fill(fillValue)
        }
        chunk = {
          data: chunkData as Chunk<D>['data'],
          shape: arr.chunks.slice(),
          stride: get_strides(arr.chunks),
        }
      } else {
        // Decode on worker
        const worker = acquireWorker(pool, resolvedWorkerUrl)
        try {
          chunk = await workerDecode<D>(worker, rawBytes, codecMeta)
          releaseWorker(pool, worker)
        } catch (error) {
          // Don't return broken workers to the pool
          worker.terminate()
          throw error
        }
      }

      // Copy decoded chunk into output on main thread
      setter.set_from_chunk(out, chunk, mapping)
    })
  }

  await queue.onIdle()

  // If the final shape is empty (all integer selections), return a scalar
  if (indexer.shape.length === 0) {
    const unwrap = 'get' in out.data
      ? (out.data as unknown as { get(idx: number): Scalar<D> }).get(0)
      : (out.data as unknown as ArrayLike<Scalar<D>>)[0]
    // @ts-expect-error: TS can't narrow conditional type
    return unwrap
  }

  // @ts-expect-error: TS can't narrow conditional type
  return out
}
