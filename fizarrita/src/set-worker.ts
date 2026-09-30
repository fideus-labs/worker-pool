/**
 * setWorker() — Worker-accelerated set for zarrita arrays.
 *
 * Writes data to a zarrita Array, offloading codec encode (and decode for
 * partial chunk updates) operations to a WorkerPool. The main thread handles
 * data modification and store writes, while workers handle the expensive
 * codec operations.
 *
 * For a sharded array the unit of codec work is still the inner chunk — one
 * task each, so a shard's inner chunks encode in parallel — while the unit
 * of store writes is the shard: the last of a shard's tasks to finish
 * assembles the shard from the new inner chunks and the untouched ones of
 * the shard as stored, and writes it.
 *
 * Uses WorkerPool.runTasks() for bounded-concurrency scheduling.
 */

import type {
  WorkerLike,
  WorkerPool,
  WorkerPoolTask,
} from "@fideus-labs/worker-pool"
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
  assemble_shard,
  type ExistingShard,
  forget_shard_index,
  inner_chunk_bytes,
  read_shard,
  shard_layout,
  shard_positions,
} from "./internals/sharding.js"
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

/** One chunk the write touches, and how the value maps into it. */
interface ChunkWrite {
  chunk_coords: number[]
  /** The chunk-side selection, per dimension. */
  chunkSelection: (number | Indices)[]
  /** The value-to-chunk projections. */
  flipped: Projection[]
  /** Whether the write replaces the whole chunk, so its old bytes are moot. */
  total: boolean
}

/**
 * A shard the write touches: the inner chunks it rewrites, and what it must
 * keep of the shard as stored.
 */
interface ShardPlan {
  path: string
  /** Inner chunks of the shard that lie within the array. */
  inBounds: number
  /** Touched inner chunks that are replaced outright. */
  total: number
  /** Touched inner chunks not yet encoded. */
  remaining: number
  /** Encoded inner chunks, by flat position in the shard. */
  written: Map<number, Uint8Array>
  /** The shard as stored, fetched once and only when some of it is kept. */
  existing?: Promise<ExistingShard | null>
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
 * Sharded arrays are written too — which zarrita's `set` refuses. A shard is
 * rewritten as a whole: inner chunks the write covers are encoded afresh,
 * the rest are copied from the shard as stored, and the index is rebuilt.
 * Two concurrent writes to the same shard race, the last one winning, as
 * two concurrent partial writes to one chunk do.
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

  // Handed to every store read: the metadata, and existing chunks or shards
  // fetched for a partial update.
  const storeOpts = signal ? { signal } : undefined

  // Read metadata from store — single read, single parse
  const { codecMeta, encodeChunkKey, fillValue, sharding } =
    await readArrayMetadata(arr, storeOpts)

  // Checkpoint for stores that ignore the signal, before any chunk is touched.
  if (signal?.aborted) {
    throw signal.reason
  }

  // Get stable metaId for the codec metadata
  const metaId = getMetaId(codecMeta)

  const Ctr = get_ctr(arr.dtype)
  const bytesPerElement = (Ctr as unknown as { BYTES_PER_ELEMENT: number })
    .BYTES_PER_ELEMENT

  // Set up the indexer. `arr.chunks` is the inner chunk shape of a sharded
  // array, so the write is planned per inner chunk either way.
  const indexer = new BasicIndexer({
    selection,
    shape: arr.shape,
    chunk_shape: arr.chunks,
  })

  // Pre-compute chunk invariants (hoisted out of loop)
  const chunkShape = arr.chunks
  const chunkStrides = get_strides(chunkShape)
  const chunkSize = chunkShape.reduce((a: number, b: number) => a * b, 1)

  const writes: ChunkWrite[] = []
  for (const { chunk_coords, mapping } of indexer) {
    const chunkSelection = mapping.map((m) => m.from)
    writes.push({
      chunk_coords,
      chunkSelection,
      flipped: mapping.map(flip_indexer_projection) as Projection[],
      total: is_total_slice(chunkSelection, chunkShape),
    })
  }

  /**
   * Encode one chunk as it stands after the write: built from the value
   * alone when the write covers it, else its bytes as stored — from
   * `existing` — or the fill value when there are none, decoded, modified,
   * and re-encoded. All codec work runs on `worker`.
   */
  const encodeChunk = async (
    worker: WorkerLike,
    write: ChunkWrite,
    existing: () => Promise<Uint8Array | undefined>,
  ): Promise<Uint8Array> => {
    let chunkData: TypedArray<D>
    // The layout of `chunkData`: C order unless it was decoded through a
    // transpose codec, which leaves the data in the codec's own order.
    // Modifying it in place and encoding it must both use that layout.
    let chunkDataStrides = chunkStrides

    if (write.total) {
      // Totally replace this chunk — no need to fetch existing data
      // Use SAB when requested so the encode worker can read without transfer
      const buffer = createBuffer(chunkSize * bytesPerElement, useShared)
      chunkData = new Ctr(buffer as ArrayBuffer, 0, chunkSize) as TypedArray<D>
      if (typeof value === "object" && value !== null) {
        const chunk = setter.prepare(
          chunkData,
          chunkShape.slice(),
          chunkStrides.slice(),
        ) as Chunk<D>
        setter.set_from_chunk(chunk, value as Chunk<D>, write.flipped)
      } else {
        // @ts-expect-error: scalar fill
        chunkData.fill(value)
      }
    } else {
      // Partial replacement — fetch and decode existing chunk first
      const rawBytes = await existing()

      if (rawBytes) {
        // Decode existing chunk on worker
        const decoded = await workerDecode<D>(worker, rawBytes, metaId, codecMeta)
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
          ;(sabData as unknown as { set(src: unknown): void }).set(decoded.data)
          chunkData = sabData
        } else {
          chunkData = decoded.data
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

      const chunk = setter.prepare(
        chunkData,
        chunkShape.slice(),
        chunkDataStrides.slice(),
      ) as Chunk<D>
      if (typeof value === "object" && value !== null) {
        setter.set_from_chunk(chunk, value as Chunk<D>, write.flipped)
      } else {
        setter.set_scalar(chunk, write.chunkSelection, value as Scalar<D>)
      }
    }

    // Encode the chunk on the worker
    const encode = useShared ? workerEncodeShared : workerEncode
    return encode<D>(worker, chunkData, metaId, codecMeta, chunkDataStrides)
  }

  /**
   * A pool task running `body` on the slot's worker — or on one made for the
   * task, when the pool lends none. A failure terminates the worker: a codec
   * that threw may have left it wedged, and a worker created here never
   * reached the pool, so nothing else would shut it down. The failure is
   * reported against `path`, the store key being written.
   */
  const task = (
    path: string,
    body: (worker: WorkerLike) => Promise<void>,
  ): WorkerPoolTask<void> => {
    return async (workerSlot: WorkerLike | null) => {
      const worker = workerSlot ?? createCodecWorker(workerUrl)
      try {
        await body(worker)
        return { worker, result: undefined as void }
      } catch (error) {
        worker.terminate()
        throw atChunk(error, path)
      }
    }
  }

  const tasks: WorkerPoolTask<void>[] = []

  if (!sharding) {
    // One task per chunk: encode it, then write it.
    for (const write of writes) {
      const chunkPath = arr.resolve(encodeChunkKey(write.chunk_coords)).path
      tasks.push(
        task(chunkPath, async (worker) => {
          const encoded = await encodeChunk(worker, write, async () =>
            arr.store.get(chunkPath as `/${string}`, storeOpts),
          )
          // Last point to back out before this chunk is committed.
          if (signal?.aborted) {
            throw signal.reason
          }
          await arr.store.set(chunkPath as `/${string}`, encoded)
        }),
      )
    }
  } else {
    // One task per inner chunk: encode it and hand it to its shard's plan.
    // The task that completes a shard assembles and writes the shard.
    const layout = shard_layout(sharding, chunkShape)
    const { grid, count } = layout
    const chunkGrid = arr.shape.map((s, i) => Math.ceil(s / chunkShape[i]))
    const plans = new Map<string, ShardPlan>()

    for (const write of writes) {
      const shard_coords = write.chunk_coords.map((c, i) =>
        Math.floor(c / grid[i]),
      )
      const local = write.chunk_coords.map((c, i) => c % grid[i])
      const flat = local.reduce((acc, l, i) => acc * grid[i] + l, 0)

      const key = shard_coords.join(",")
      let plan = plans.get(key)
      if (!plan) {
        plan = {
          path: arr.resolve(encodeChunkKey(shard_coords)).path,
          inBounds: grid.reduce(
            (n, g, i) =>
              n * Math.max(0, Math.min(g, chunkGrid[i] - shard_coords[i] * g)),
            1,
          ),
          total: 0,
          remaining: 0,
          written: new Map(),
        }
        plans.set(key, plan)
      }
      const shard = plan
      shard.remaining++
      if (write.total) shard.total++

      tasks.push(
        task(shard.path, async (worker) => {
          // Fetched at most once per shard, by whichever task first needs it:
          // a partial inner chunk, or the assembly keeping untouched ones.
          const existing = () =>
            (shard.existing ??= read_shard(
              arr.store,
              shard.path,
              sharding,
              layout,
              storeOpts,
            ))
          const encoded = await encodeChunk(worker, write, async () => {
            const stored = await existing()
            return stored ? inner_chunk_bytes(stored, local, shard.path) : undefined
          })
          shard.written.set(flat, encoded)
          if (--shard.remaining > 0) return

          // The shard's last inner chunk. The shard as stored is needed
          // unless every inner chunk within the array was replaced outright;
          // the counts are final, every task having been planned before any
          // ran.
          const stored = shard.total < shard.inBounds ? await existing() : null
          const parts: (Uint8Array | undefined)[] = new Array(count)
          for (const { local, flat } of shard_positions(grid)) {
            parts[flat] =
              shard.written.get(flat) ??
              (stored ? inner_chunk_bytes(stored, local, shard.path) : undefined)
          }
          // Last point to back out before this shard is committed.
          if (signal?.aborted) {
            throw signal.reason
          }
          try {
            await arr.store.set(
              shard.path as `/${string}`,
              assemble_shard(parts, sharding),
            )
          } finally {
            // Readers remember shard indexes, and this one may have changed —
            // also when the store rejected after writing some or all of it.
            forget_shard_index(arr.store, shard.path)
          }
        }),
      )
    }
  }

  // Execute all tasks with bounded concurrency via WorkerPool, which drops
  // still-queued tasks when the signal fires.
  if (tasks.length > 0) {
    const { promise } = pool.runTasks(tasks, null, { signal })
    await promise
  }
}
