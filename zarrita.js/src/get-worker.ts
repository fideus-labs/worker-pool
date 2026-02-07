/**
 * getWorker() — Worker-accelerated get for zarrita arrays.
 *
 * Reads data from a zarrita Array, offloading codec decode operations to a
 * WorkerPool. The main thread fetches raw bytes from the store, transfers
 * them to a worker for decoding, then copies the decoded chunk into the
 * output array on the main thread.
 *
 * Uses WorkerPool.runTasks() for bounded-concurrency scheduling.
 */

import type { WorkerPool, WorkerPoolTask } from '@fideus-labs/worker-pool'
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
import {
  get_ctr,
  get_strides,
  create_chunk_key_encoder,
  assertSharedArrayBufferAvailable,
  createBuffer,
} from './internals/util.js'
import type { GetWorkerOptions, CodecChunkMeta } from './types.js'
import { workerDecode, workerDecodeInto, getMetaId } from './worker-rpc.js'

/**
 * Default URL for the codec worker. Uses `import.meta.url` to resolve
 * relative to this module.
 */
const DEFAULT_WORKER_URL = new URL('./codec-worker.js', import.meta.url)

/** Shared TextDecoder instance. */
const decoder = new TextDecoder()

// ---------------------------------------------------------------------------
// Unified metadata reader — reads zarr.json once, returns everything needed
// ---------------------------------------------------------------------------

interface ArrayMetadata {
  codecMeta: CodecChunkMeta
  encodeChunkKey: (chunk_coords: number[]) => string
  fillValue: Scalar<DataType> | null
}

async function readArrayMetadata<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
): Promise<ArrayMetadata> {
  const store = arr.store

  // Try v3 first: read zarr.json
  const v3Path = (arr.path === '/' ? '/zarr.json' : `${arr.path}/zarr.json`) as `/${string}`
  const v3Bytes = await store.get(v3Path)
  if (v3Bytes) {
    const metadata = JSON.parse(decoder.decode(v3Bytes))
    return {
      codecMeta: {
        data_type: metadata.data_type,
        chunk_shape: metadata.chunk_grid.configuration.chunk_shape,
        codecs: metadata.codecs,
      },
      encodeChunkKey: create_chunk_key_encoder(metadata.chunk_key_encoding),
      fillValue: metadata.fill_value ?? null,
    }
  }

  // Try v2: read .zarray
  const v2Path = (arr.path === '/' ? '/.zarray' : `${arr.path}/.zarray`) as `/${string}`
  const v2Bytes = await store.get(v2Path)
  if (v2Bytes) {
    const metadata = JSON.parse(decoder.decode(v2Bytes))
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
      codecMeta: {
        data_type: arr.dtype,
        chunk_shape: arr.chunks,
        codecs: codecs.length > 0 ? codecs : [{ name: 'bytes', configuration: { endian: 'little' } }],
      },
      encodeChunkKey: create_chunk_key_encoder({
        name: 'v2',
        configuration: { separator: metadata.dimension_separator ?? '.' },
      }),
      fillValue: metadata.fill_value ?? null,
    }
  }

  // Fallback: BytesCodec only, default v3 key encoding
  return {
    codecMeta: {
      data_type: arr.dtype,
      chunk_shape: arr.chunks,
      codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
    },
    encodeChunkKey: create_chunk_key_encoder({ name: 'default' }),
    fillValue: null,
  }
}

// ---------------------------------------------------------------------------
// getWorker
// ---------------------------------------------------------------------------

/**
 * Read data from a zarrita Array with codec decoding offloaded to Web Workers.
 *
 * Drop-in replacement for zarrita's `get()` with worker acceleration.
 * The main thread fetches raw bytes from the store, then workers handle
 * the (potentially expensive) codec decode operations in parallel.
 *
 * @param arr       - The zarrita Array to read from.
 * @param selection - Index selection (null for full array, or per-dimension slices/indices).
 * @param opts      - Options including the WorkerPool and store options.
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
  const { pool, workerUrl } = opts
  const resolvedWorkerUrl = workerUrl ?? DEFAULT_WORKER_URL
  const useShared = !!opts.useSharedArrayBuffer

  if (useShared) {
    assertSharedArrayBufferAvailable()
  }

  // Read metadata from store — single read, single parse
  const { codecMeta, encodeChunkKey, fillValue } = await readArrayMetadata(arr)

  // Get stable metaId for the codec metadata (used by worker-rpc meta-init)
  const metaId = getMetaId(codecMeta)

  const Ctr = get_ctr(arr.dtype)
  const bytesPerElement = (Ctr as unknown as { BYTES_PER_ELEMENT: number }).BYTES_PER_ELEMENT

  // Set up the indexer
  const indexer = new BasicIndexer({
    selection,
    shape: arr.shape,
    chunk_shape: arr.chunks,
  })

  // Allocate output — backed by SharedArrayBuffer when requested
  const size = indexer.shape.reduce((a: number, b: number) => a * b, 1)
  const buffer = createBuffer(size * bytesPerElement, useShared)
  const data = new Ctr(buffer as ArrayBuffer, 0, size)
  const outStride = get_strides(indexer.shape)
  const out = setter.prepare(data, indexer.shape, outStride) as Chunk<D>

  // Pre-compute chunk invariants (hoisted out of loop)
  const chunkShape = arr.chunks
  const chunkStrides = get_strides(chunkShape)
  const chunkSize = chunkShape.reduce((a: number, b: number) => a * b, 1)

  // Build tasks — one per chunk
  const tasks: WorkerPoolTask<void>[] = []

  for (const { chunk_coords, mapping } of indexer) {
    const chunkKey = encodeChunkKey(chunk_coords)
    const chunkPath = arr.resolve(chunkKey).path

    tasks.push(async (workerSlot: Worker | null) => {
      const worker = workerSlot ?? new Worker(resolvedWorkerUrl, { type: 'module' })

      // Fetch raw bytes from store on main thread
      const rawBytes = await arr.store.get(chunkPath, opts.opts)

      if (!rawBytes) {
        // Missing chunk — fill value, no worker needed
        const chunkData = new Ctr(chunkSize)
        if (fillValue != null) {
          // @ts-expect-error: fill_value type is union
          chunkData.fill(fillValue)
        }
        const chunk: Chunk<D> = {
          data: chunkData as Chunk<D>['data'],
          shape: chunkShape.slice(),
          stride: chunkStrides.slice(),
        }
        // Copy fill-value chunk into output on main thread
        setter.set_from_chunk(out, chunk, mapping)
      } else if (useShared) {
        // Decode-into-shared: worker decodes AND writes directly into
        // the SharedArrayBuffer output — no transfer back, no main-thread copy
        try {
          await workerDecodeInto(
            worker,
            rawBytes,
            metaId,
            codecMeta,
            buffer as SharedArrayBuffer,
            size * bytesPerElement,
            outStride,
            mapping,
            bytesPerElement,
          )
        } catch (error) {
          worker.terminate()
          throw error
        }
      } else {
        // Standard path: worker decodes, transfers back, main thread copies
        let chunk: Chunk<D>
        try {
          chunk = await workerDecode<D>(worker, rawBytes, metaId, codecMeta)
        } catch (error) {
          worker.terminate()
          throw error
        }
        setter.set_from_chunk(out, chunk, mapping)
      }

      return { worker, result: undefined as void }
    })
  }

  // Execute all tasks with bounded concurrency via WorkerPool
  if (tasks.length > 0) {
    const { promise } = pool.runTasks(tasks)
    await promise
  }

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
