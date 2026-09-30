/**
 * Sharded arrays (`sharding_indexed`) — locating inner chunks inside shards.
 *
 * A shard is one chunk of the array's outer chunk grid, stored under that
 * chunk's key. Inside it, inner chunks encoded with the sharding codec's own
 * `codecs` are concatenated, and an index — a uint64 array of shape
 * `[...innerChunksPerShard, 2]` holding `(offset, length)` per inner chunk,
 * `2^64 - 1` for both when the chunk is absent — is encoded with
 * `index_codecs` and placed at the shard's end (or start).
 *
 * The codec worker never sees a shard: it decodes inner chunks with the
 * inner codecs, exactly as it decodes the chunks of an unsharded array. What
 * differs is only where the bytes come from, and that is what this module
 * provides. Adapted from zarrita.js/src/codecs/sharding.ts (0.7), which
 * reads shards the same way.
 */

import { InvalidMetadataError, UnsupportedError } from 'zarrita'
import type { CodecMetadata, GetOptions, Readable } from 'zarrita'

import { separateSignal, untilAborted } from './abort.js'
import { create_codec_pipeline } from './codec-pipeline.js'

/** Name of the sharding codec in a v3 array's `codecs` list. */
export const SHARDING_CODEC = 'sharding_indexed'

/** What a reader needs, beyond the inner chunk shape, to find inner chunks. */
export interface ShardingInfo {
  /** Extent of one shard: the outer chunk grid's `chunk_shape`. */
  shard_shape: number[]
  /** Codecs the shard index is encoded with; all of fixed output size. */
  index_codecs: CodecMetadata[]
  /** Which end of the shard the index sits at. */
  index_location: 'start' | 'end'
}

/**
 * Split a sharded array's metadata into what the codec worker decodes — the
 * inner chunk shape and codecs — and what the reader needs to find inner
 * chunks. The configuration is the codec's, straight from `zarr.json`, and
 * is checked here rather than trusted.
 */
export function resolve_sharding(
  shard_shape: number[],
  configuration: Record<string, unknown> | undefined,
): { chunk_shape: number[]; codecs: CodecMetadata[]; sharding: ShardingInfo } {
  const chunk_shape = configuration?.chunk_shape as number[]
  const codecs = configuration?.codecs as CodecMetadata[]
  const index_codecs = configuration?.index_codecs as CodecMetadata[]
  const index_location = (configuration?.index_location ?? 'end') as
    | 'start'
    | 'end'
  if (index_location !== 'start' && index_location !== 'end') {
    throw new InvalidMetadataError(
      `Invalid sharding_indexed index_location: ${JSON.stringify(index_location)}`,
    )
  }
  if (
    !Array.isArray(chunk_shape) ||
    chunk_shape.length !== shard_shape.length ||
    chunk_shape.some((c, i) => !(c > 0) || shard_shape[i] % c !== 0)
  ) {
    throw new InvalidMetadataError(
      `sharding_indexed chunk_shape ${JSON.stringify(chunk_shape)} does not ` +
        `evenly divide the shard shape ${JSON.stringify(shard_shape)}`,
    )
  }
  if (!Array.isArray(codecs) || !Array.isArray(index_codecs)) {
    throw new InvalidMetadataError(
      'sharding_indexed configuration needs `codecs` and `index_codecs` lists',
    )
  }
  return { chunk_shape, codecs, sharding: { shard_shape, index_codecs, index_location } }
}

// ---------------------------------------------------------------------------
// Chunk sources
// ---------------------------------------------------------------------------

/** Where a chunk's bytes live in the store, and how to fetch them. */
export interface ChunkSource {
  /** The store key the bytes are read from — for a sharded array, the shard's. */
  path: string
  /** The chunk's encoded bytes, or `undefined` when the store has none. */
  fetch(): Promise<Uint8Array | undefined>
}

/** A decoded shard index: `(offset, length)` pairs in a `[...grid, 2]` array. */
interface ShardIndex {
  data: BigUint64Array
  shape: number[]
  stride: number[]
}

const MAX_UINT64 = 0xffffffffffffffffn
const MAX_SAFE_UINT64 = BigInt(Number.MAX_SAFE_INTEGER)

/**
 * Decoded shard indexes, per store and shard path, for the store's lifetime.
 *
 * An index is a few bytes at the end of a shard, but fetching it is a store
 * round-trip, and every inner chunk of the shard — across every read of the
 * array — needs it. Cached for as long as the store lives, like zarrita
 * caches it for as long as the `Array` lives, and like {@link resolveArrayInfo}
 * memoises the metadata: a shard's index changes only when the shard is
 * rewritten, which `setWorker` refuses to do. A rejected fetch is evicted, so
 * a transient failure is retried by the next read rather than kept.
 */
const shardIndexes = new WeakMap<object, Map<string, Promise<ShardIndex | null>>>()

/**
 * A chunk source for an unsharded array: the chunk's own store key, fetched
 * with `store.get`.
 */
export function createChunkSource<Store extends Readable>(
  arr: { store: Store; resolve(key: string): { path: string } },
  encodeChunkKey: (chunk_coords: number[]) => string,
  storeOpts?: Parameters<Store['get']>[1],
): (chunk_coords: number[]) => ChunkSource {
  return (chunk_coords) => {
    const path = arr.resolve(encodeChunkKey(chunk_coords)).path
    return {
      path,
      async fetch() {
        return arr.store.get(path as `/${string}`, storeOpts)
      },
    }
  }
}

/**
 * A chunk source for a sharded array: the inner chunk's slice of its shard.
 *
 * The shard index is range-fetched once per shard (see {@link shardIndexes})
 * and each inner chunk is one range request — so `zarr.withRangeCoalescing`
 * batches the requests of neighbouring inner chunks, and
 * `zarr.withByteCaching` caches them. The shared index fetch runs without the
 * caller's `AbortSignal`, as the shared metadata read does: the caller's
 * signal governs its own wait, and cannot fail the other readers sharing the
 * index.
 *
 * The store must have `getRange`. zarrita will not open a sharded array on a
 * store without it, so no such array reaches here; the check is kept for the
 * error to be zarrita's if one ever does.
 *
 * `encodeShardKey` encodes *shard* coordinates: an inner chunk has no key of
 * its own.
 */
export function createShardedChunkSource<Store extends Readable>(
  arr: { store: Store; resolve(key: string): { path: string } },
  encodeShardKey: (shard_coords: number[]) => string,
  chunk_shape: number[],
  sharding: ShardingInfo,
  storeOpts?: Parameters<Store['get']>[1],
): (chunk_coords: number[]) => ChunkSource {
  const { store } = arr
  if (!store.getRange) {
    throw new UnsupportedError('sharding requires a store with getRange')
  }
  const getRange = store.getRange.bind(store)

  // Inner chunks per shard along each axis.
  const grid = sharding.shard_shape.map((s, i) => s / chunk_shape[i])
  const index_pipeline = create_codec_pipeline({
    data_type: 'uint64',
    shape: [...grid, 2],
    codecs: sharding.index_codecs,
  })
  const raw_index_size = 16 * grid.reduce((a, b) => a * b, 1)

  // The index fetch is shared by every reader of the shard, so it runs on
  // the options they have in common; each caller's own signal governs its
  // wait below. Inner chunk fetches are the caller's own and carry it.
  const { shared, signal } = separateSignal(storeOpts)
  let byPath = shardIndexes.get(store)
  if (!byPath) {
    byPath = new Map()
    shardIndexes.set(store, byPath)
  }
  const indexes = byPath

  const index_for = (shard_path: string): Promise<ShardIndex | null> => {
    let promise = indexes.get(shard_path)
    if (!promise) {
      promise = (async () => {
        const size = await index_pipeline.computeEncodedSize(raw_index_size)
        const range =
          sharding.index_location === 'end'
            ? { suffixLength: size }
            : { offset: 0, length: size }
        const bytes = await getRange(
          shard_path as `/${string}`,
          range,
          shared as GetOptions,
        )
        if (!bytes) return null
        // Copied before decoding: a range may be a view starting at any
        // byte, and a uint64 view needs an 8-byte-aligned start.
        return (await index_pipeline.decode(bytes.slice())) as unknown as ShardIndex
      })()
      indexes.set(shard_path, promise)
      const forget = () => {
        if (indexes.get(shard_path) === promise) indexes.delete(shard_path)
      }
      promise.catch(forget)
    }
    return untilAborted(promise, signal)
  }

  return (chunk_coords) => {
    const shard_coords = chunk_coords.map((c, i) => Math.floor(c / grid[i]))
    const path = arr.resolve(encodeShardKey(shard_coords)).path
    return {
      path,
      async fetch() {
        const index = await index_for(path)
        if (index === null) return undefined
        // The inner chunk's position within the shard, as an offset into the
        // index: its `(offset, length)` pair sits at that offset and the next.
        const entry = chunk_coords.reduce(
          (acc, c, i) => acc + (c % grid[i]) * index.stride[i],
          0,
        )
        const offset = index.data[entry]
        const length = index.data[entry + 1]
        if (offset === MAX_UINT64 && length === MAX_UINT64) return undefined
        // Anything a number cannot hold exactly is not a position in a
        // shard; `Number()` would round it rather than refuse it.
        if (offset > MAX_SAFE_UINT64 || length > MAX_SAFE_UINT64) {
          throw new InvalidMetadataError(
            `Shard ${path} index entry for inner chunk [${chunk_coords}] ` +
              `is out of range: offset ${offset}, length ${length}`,
          )
        }
        return getRange(
          path as `/${string}`,
          { offset: Number(offset), length: Number(length) },
          storeOpts as GetOptions,
        )
      },
    }
  }
}
