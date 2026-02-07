/**
 * set() — Writes data to a ZarrArray using a ChunkQueue for scheduling.
 *
 * Ported from zarrita.js packages/zarrita/src/indexing/set.ts
 *
 * Integration with WorkerPool:
 *   - Main thread modifies chunk data (set_scalar / set_from_chunk)
 *   - Worker encodes the chunk (via the codec-worker)
 *   - Main thread writes encoded bytes to the store
 *
 * When using the default queue (no WorkerPool), encoding happens
 * synchronously on the main thread via the ZarrArray's built-in codec.
 */

import type { Chunk, DataType, Indices, Scalar, SetOptions, Slice, TypedArrayFor } from './types.js'
import { BasicIndexer, type IndexerProjection } from './indexer.js'
import { setter } from './setter.js'
import { create_queue, get_ctr, get_strides } from './util.js'
import type { ZarrArray } from './store.js'

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
// set()
// ---------------------------------------------------------------------------

/**
 * Write data to a ZarrArray, optionally using a custom ChunkQueue.
 *
 * @param arr       - The ZarrArray to write to.
 * @param selection - Index selection (null for entire array).
 * @param value     - Scalar value or Chunk to write.
 * @param opts      - Options including optional `create_queue` factory.
 */
export async function set<D extends DataType>(
  arr: ZarrArray<D>,
  selection: (number | Slice | null)[] | null,
  value: Scalar<D> | Chunk<D>,
  opts: SetOptions = {},
): Promise<void> {
  const indexer = new BasicIndexer({
    selection,
    shape: arr.shape,
    chunk_shape: arr.chunks,
  })

  const chunk_size = arr.chunks.reduce((a, b) => a * b, 1)
  const queue = opts.create_queue ? opts.create_queue() : create_queue()

  const Ctr = get_ctr(arr.dtype)

  for (const { chunk_coords, mapping } of indexer) {
    const chunk_selection = mapping.map((i) => i.from)
    const flipped = mapping.map(flip_indexer_projection)

    queue.add(async () => {
      const chunk_path = arr.resolve(arr.encodeChunkKey(chunk_coords)).path
      const chunk_shape = arr.chunks.slice()
      const chunk_stride = get_strides(chunk_shape)

      let chunk_data: TypedArrayFor<D>

      if (is_total_slice(chunk_selection, chunk_shape)) {
        // Totally replace this chunk
        chunk_data = new Ctr(chunk_size) as TypedArrayFor<D>
        if (typeof value === 'object' && value !== null) {
          const chunk = setter.prepare(chunk_data, chunk_shape.slice(), chunk_stride.slice()) as Chunk<D>
          setter.set_from_chunk(
            chunk as Chunk<DataType>,
            value as Chunk<DataType>,
            flipped as Array<{ from: Indices | number | null; to: Indices | number | null }>,
          )
        } else {
          ;(chunk_data as unknown as { fill(v: number): void }).fill(value as number)
        }
      } else {
        // Partially replace — fetch existing chunk first
        chunk_data = (await arr.getChunk(chunk_coords)).data
        const chunk = setter.prepare(chunk_data, chunk_shape.slice(), chunk_stride.slice()) as Chunk<D>
        if (typeof value === 'object' && value !== null) {
          setter.set_from_chunk(
            chunk as Chunk<DataType>,
            value as Chunk<DataType>,
            flipped as Array<{ from: Indices | number | null; to: Indices | number | null }>,
          )
        } else {
          setter.set_scalar(
            chunk as Chunk<DataType>,
            chunk_selection as (number | Indices)[],
            value as number,
          )
        }
      }

      // Encode and write to store
      const encoded = arr.codec.encode({
        data: chunk_data,
        shape: chunk_shape,
        stride: chunk_stride,
      } as Chunk<D>)
      arr.store.set(chunk_path, encoded)
    })
  }

  await queue.onIdle()
}
