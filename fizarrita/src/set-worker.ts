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

import type {
  WorkerLike,
  WorkerPool,
  WorkerPoolTask,
} from "@fideus-labs/worker-pool"
import { UnsupportedError } from "zarrita"
import type {
  Chunk,
  DataType,
  Indices,
  Mutable,
  Projection,
  Scalar,
  Slice,
  TypedArray,
  Array as ZarrArray,
} from "zarrita"

import { createCodecWorker } from "./create-worker.js"
import { readArrayMetadata } from "./get-worker.js"
import { BasicIndexer, type IndexerProjection } from "./internals/indexer.js"
import { setter } from "./internals/setter.js"
import {
  assertSharedArrayBufferAvailable,
  createBuffer,
  get_ctr,
  get_strides,
} from "./internals/util.js"
import type { SetWorkerOptions } from "./types.js"
import {
  atChunk,
  getMetaId,
  workerDecode,
  workerEncode,
  workerEncodeShared,
} from "./worker-rpc.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function flip_indexer_projection(m: IndexerProjection): {
  from: number | Indices | null
  to: number | Indices | null
} {
  if (m.to == null) return { from: m.to, to: m.from }
  return { from: m.to, to: m.from }
}

function is_total_slice(
  selection: (number | Indices)[],
  shape: readonly number[],
): boolean {
  return selection.every((s, i) => {
    if (typeof s === "number") return false
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
 *   chunkShape: [10, 10],
 *   dtype: 'float32',
 * })
 *
 * try {
 *   await setWorker(arr, null, 42.0, { pool })
 * } finally {
 *   pool.terminateWorkers()
 * }
 * ```
 */
export async function setWorker<D extends DataType>(
  arr: ZarrArray<D, Mutable>,
  selection: (number | Slice | null)[] | null,
  value: Scalar<D> | Chunk<D>,
  opts: SetWorkerOptions,
): Promise<void> {
  const { pool, workerUrl, signal } = opts
  const useShared = !!opts.useSharedArrayBuffer

  // Not `throwIfAborted()` — see getWorker.
  if (signal?.aborted) {
    throw signal.reason
  }

  if (useShared) {
    assertSharedArrayBufferAvailable()
  }

  // Handed to every store read: the metadata, and existing chunks fetched for
  // a partial update.
  const storeOpts = signal ? { signal } : undefined

  // Read metadata from store — single read, single parse
  const { codecMeta, encodeChunkKey, fillValue, sharding } =
    await readArrayMetadata(arr, storeOpts)

  // As zarrita: a shard is rewritten as a whole, index and all, and neither
  // implements that.
  if (sharding) {
    throw new UnsupportedError("set on sharded arrays")
  }

  // Checkpoint for stores that ignore the signal, before any chunk is touched.
  if (signal?.aborted) {
    throw signal.reason
  }

  // Get stable metaId for the codec metadata
  const metaId = getMetaId(codecMeta)

  const Ctr = get_ctr(arr.dtype)
  const bytesPerElement = (Ctr as unknown as { BYTES_PER_ELEMENT: number })
    .BYTES_PER_ELEMENT

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

    tasks.push(async (workerSlot: WorkerLike | null) => {
      const worker = workerSlot ?? createCodecWorker(workerUrl)

      let chunkData: TypedArray<D>
      // The layout of `chunkData`: C order unless it was decoded through a
      // transpose codec, which leaves the data in the codec's own order.
      // Modifying it in place and encoding it must both use that layout.
      let chunkDataStrides = chunkStrides

      if (is_total_slice(chunkSelection, chunkShape)) {
        // Totally replace this chunk — no need to fetch existing data
        // Use SAB when requested so the encode worker can read without transfer
        const buffer = createBuffer(chunkSize * bytesPerElement, useShared)
        chunkData = new Ctr(
          buffer as ArrayBuffer,
          0,
          chunkSize,
        ) as TypedArray<D>
        if (typeof value === "object" && value !== null) {
          const chunk = setter.prepare(
            chunkData,
            chunkShape.slice(),
            chunkStrides.slice(),
          ) as Chunk<D>
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
        const rawBytes = await arr.store.get(
          chunkPath as `/${string}`,
          storeOpts,
        )

        if (rawBytes) {
          // Decode existing chunk on worker
          try {
            const decoded = await workerDecode<D>(
              worker,
              rawBytes,
              metaId,
              codecMeta,
            )
            chunkDataStrides = decoded.stride
            if (useShared) {
              // Copy decoded data into a SAB-backed buffer for zero-transfer encode
              const buffer = createBuffer(
                chunkSize * bytesPerElement,
                true,
              ) as SharedArrayBuffer
              const sabData = new Ctr(
                buffer as unknown as ArrayBuffer,
                0,
                chunkSize,
              ) as TypedArray<D>
              ;(sabData as unknown as { set(src: unknown): void }).set(
                decoded.data,
              )
              chunkData = sabData
            } else {
              chunkData = decoded.data
            }
          } catch (error) {
            worker.terminate()
            throw atChunk(error, chunkPath)
          }
        } else {
          // Missing chunk — start from fill value
          const buffer = createBuffer(chunkSize * bytesPerElement, useShared)
          chunkData = new Ctr(
            buffer as ArrayBuffer,
            0,
            chunkSize,
          ) as TypedArray<D>
          if (fillValue != null) {
            // @ts-expect-error: fill_value union
            chunkData.fill(fillValue)
          }
        }

        const chunk = setter.prepare(
          chunkData,
          chunkShape.slice(),
          chunkDataStrides.slice(),
        ) as Chunk<D>
        if (typeof value === "object" && value !== null) {
          setter.set_from_chunk(
            chunk,
            value as Chunk<D>,
            flipped as Projection[],
          )
        } else {
          setter.set_scalar(
            chunk,
            chunkSelection as (number | Indices)[],
            value as Scalar<D>,
          )
        }
      }

      // Encode the chunk on the worker
      try {
        const encode = useShared ? workerEncodeShared : workerEncode
        const encoded = await encode<D>(
          worker,
          chunkData,
          metaId,
          codecMeta,
          chunkDataStrides,
        )

        // Last point to back out before this chunk is committed.
        if (signal?.aborted) {
          throw signal.reason
        }

        // Write to store on main thread
        await arr.store.set(chunkPath as `/${string}`, encoded)
      } catch (error) {
        worker.terminate()
        throw atChunk(error, chunkPath)
      }

      return { worker, result: undefined as void }
    })
  }

  // Execute all tasks with bounded concurrency via WorkerPool, which drops
  // still-queued tasks when the signal fires.
  if (tasks.length > 0) {
    const { promise } = pool.runTasks(tasks, null, { signal })
    await promise
  }
}
