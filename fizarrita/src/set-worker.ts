/**
 * setWorker() — Worker-accelerated set for zarrita arrays.
 *
 * Writes data to a zarrita Array, offloading codec encode (and decode for
 * partial chunk updates) operations to a WorkerPool. The main thread handles
 * data modification and store writes, while workers handle the expensive
 * codec operations.
 *
 * Uses WorkerPool.runTasks() for bounded-concurrency scheduling.
 */

import type { WorkerPool, WorkerPoolTask } from '@fideus-labs/worker-pool'
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
import {
  get_ctr,
  get_strides,
  create_chunk_key_encoder,
  assertSharedArrayBufferAvailable,
  createBuffer,
} from './internals/util.js'
import type { SetWorkerOptions, CodecChunkMeta } from './types.js'
import { workerDecode, workerEncode, workerEncodeShared, getMetaId } from './worker-rpc.js'

/**
 * Default URL for the codec worker.
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

async function readArrayMetadata<D extends DataType>(
  arr: ZarrArray<D, Mutable>,
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
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// setWorker
// ---------------------------------------------------------------------------

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
 * @param opts      - Options including the WorkerPool.
 *
 * @example
 * ```ts
 * import { WorkerPool } from '@fideus-labs/worker-pool'
 * import { setWorker } from '@fideus-labs/fizarrita'
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
  const { pool, workerUrl } = opts
  const resolvedWorkerUrl = workerUrl ?? DEFAULT_WORKER_URL
  const useShared = !!opts.useSharedArrayBuffer

  if (useShared) {
    assertSharedArrayBufferAvailable()
  }

  // Read metadata from store — single read, single parse
  const { codecMeta, encodeChunkKey, fillValue } = await readArrayMetadata(arr)

  // Get stable metaId for the codec metadata
  const metaId = getMetaId(codecMeta)

  const Ctr = get_ctr(arr.dtype)
  const bytesPerElement = (Ctr as unknown as { BYTES_PER_ELEMENT: number }).BYTES_PER_ELEMENT

  // Set up the indexer
  const indexer = new BasicIndexer({
    selection,
    shape: arr.shape,
    chunk_shape: arr.chunks,
  })

  // Pre-compute chunk invariants (hoisted out of loop)
  const chunkShape = arr.chunks
  const chunkStrides = get_strides(chunkShape)
  const chunkSize = chunkShape.reduce((a: number, b: number) => a * b, 1)

  // Build tasks — one per chunk
  const tasks: WorkerPoolTask<void>[] = []

  for (const { chunk_coords, mapping } of indexer) {
    const chunkSelection = mapping.map((m) => m.from)
    const flipped = mapping.map(flip_indexer_projection)
    const chunkKey = encodeChunkKey(chunk_coords)
    const chunkPath = arr.resolve(chunkKey).path

    tasks.push(async (workerSlot: Worker | null) => {
      const worker = workerSlot ?? new Worker(resolvedWorkerUrl, { type: 'module' })

      let chunkData: TypedArray<D>

      if (is_total_slice(chunkSelection, chunkShape)) {
        // Totally replace this chunk — no need to fetch existing data
        // Use SAB when requested so the encode worker can read without transfer
        const buffer = createBuffer(chunkSize * bytesPerElement, useShared)
        chunkData = new Ctr(buffer as ArrayBuffer, 0, chunkSize) as TypedArray<D>
        if (typeof value === 'object' && value !== null) {
          const chunk = setter.prepare(chunkData, chunkShape.slice(), chunkStrides.slice()) as Chunk<D>
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
          try {
            const decoded = await workerDecode<D>(worker, rawBytes, metaId, codecMeta)
            if (useShared) {
              // Copy decoded data into a SAB-backed buffer for zero-transfer encode
              const buffer = createBuffer(
                chunkSize * bytesPerElement,
                true,
              ) as SharedArrayBuffer
              const sabData = new Ctr(buffer as unknown as ArrayBuffer, 0, chunkSize) as TypedArray<D>
              ;(sabData as unknown as { set(src: unknown): void }).set(decoded.data)
              chunkData = sabData
            } else {
              chunkData = decoded.data
            }
          } catch (error) {
            worker.terminate()
            throw error
          }
        } else {
          // Missing chunk — start from fill value
          const buffer = createBuffer(chunkSize * bytesPerElement, useShared)
          chunkData = new Ctr(buffer as ArrayBuffer, 0, chunkSize) as TypedArray<D>
          if (fillValue != null) {
            // @ts-expect-error: fill_value union
            chunkData.fill(fillValue)
          }
        }

        const chunk = setter.prepare(chunkData, chunkShape.slice(), chunkStrides.slice()) as Chunk<D>
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

      // Encode the chunk on the worker
      try {
        const encode = useShared ? workerEncodeShared : workerEncode
        const encoded = await encode<D>(worker, chunkData, metaId, codecMeta)

        // Write to store on main thread
        await arr.store.set(chunkPath as `/${string}`, encoded)
      } catch (error) {
        worker.terminate()
        throw error
      }

      return { worker, result: undefined as void }
    })
  }

  // Execute all tasks with bounded concurrency via WorkerPool
  if (tasks.length > 0) {
    const { promise } = pool.runTasks(tasks)
    await promise
  }
}
