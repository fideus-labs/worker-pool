/**
 * get() — Reads data from a ZarrArray using a ChunkQueue for scheduling.
 *
 * Ported from zarrita.js packages/zarrita/src/indexing/get.ts
 *
 * Integration with WorkerPool:
 *   - Main thread fetches raw bytes from the store
 *   - Worker decodes the bytes (via the codec-worker)
 *   - Main thread copies the decoded chunk into the output array
 *
 * When using the default queue (no WorkerPool), decoding happens
 * synchronously on the main thread via the ZarrArray's built-in codec.
 */

import type { Chunk, DataType, GetOptions, Scalar, Slice, TypedArrayFor } from './types.js'
import { BasicIndexer } from './indexer.js'
import { setter } from './setter.js'
import { create_queue, get_ctr, get_strides } from './util.js'
import type { ZarrArray } from './store.js'

/**
 * Read array data, optionally using a custom ChunkQueue for concurrency.
 *
 * @param arr       - The ZarrArray to read from.
 * @param selection - Index selection (null for entire array, or array of
 *                    Slice/number/null per dimension).
 * @param opts      - Options including optional `create_queue` factory.
 * @returns The result chunk, or a scalar if all dimensions are integer-indexed.
 */
export async function get<D extends DataType>(
  arr: ZarrArray<D>,
  selection: null | undefined | (null | Slice | number)[],
  opts: GetOptions = {},
): Promise<Chunk<D> | Scalar<D>> {
  const indexer = new BasicIndexer({
    selection: selection ?? null,
    shape: arr.shape,
    chunk_shape: arr.chunks,
  })

  const size = indexer.shape.reduce((a, b) => a * b, 1)
  const Ctr = get_ctr(arr.dtype)
  const data = new Ctr(size) as TypedArrayFor<D>
  const outStride = get_strides(indexer.shape)
  const out = setter.prepare(data, indexer.shape, outStride) as Chunk<D>

  const queue = opts.create_queue?.() ?? create_queue()

  for (const { chunk_coords, mapping } of indexer) {
    queue.add(async () => {
      const { data: chunkData, shape, stride } = await arr.getChunk(chunk_coords)
      const chunk = setter.prepare(chunkData, shape, stride) as Chunk<D>
      setter.set_from_chunk(out as Chunk<DataType>, chunk as Chunk<DataType>, mapping)
    })
  }

  await queue.onIdle()

  // If the final shape is empty (all integer selections), return a scalar.
  if (indexer.shape.length === 0) {
    return data[0] as unknown as Scalar<D>
  }
  return out
}
