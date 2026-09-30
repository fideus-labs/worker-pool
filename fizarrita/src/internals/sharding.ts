/**
 * Sharded arrays (`sharding_indexed`) — locating inner chunks inside shards,
 * and building shards.
 *
 * A shard is one chunk of the array's outer chunk grid, stored under that
 * chunk's key. Inside it, inner chunks encoded with the sharding codec's own
 * `codecs` are concatenated, and an index — a uint64 array of shape
 * `[...innerChunksPerShard, 2]` holding `(offset, length)` per inner chunk,
 * `2^64 - 1` for both when the chunk is absent — is encoded with
 * `index_codecs` and placed at the shard's end (or start).
 *
 * The codec worker never sees a shard: it decodes and encodes inner chunks
 * with the inner codecs, exactly as it handles the chunks of an unsharded
 * array. What differs is only where the bytes come from and go to, and that
 * is what this module provides. Reading is adapted from
 * zarrita.js/src/codecs/sharding.ts (0.7); zarrita does not write shards.
 */

import { InvalidMetadataError, UnsupportedError } from 'zarrita'
import type { CodecMetadata, GetOptions, Readable } from 'zarrita'

import { separateSignal, untilAborted } from './abort.js'
import { create_codec_pipeline } from './codec-pipeline.js'
import { atChunk } from './errors.js'
import { byteswap_inplace, system_is_little_endian } from './util.js'

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
// Shard layout and index
// ---------------------------------------------------------------------------

/** A decoded shard index: `(offset, length)` pairs in a `[...grid, 2]` array. */
export interface ShardIndex {
  data: BigUint64Array
  shape: number[]
  stride: number[]
}

const MAX_UINT64 = 0xffffffffffffffffn
const MAX_SAFE_UINT64 = BigInt(Number.MAX_SAFE_INTEGER)

/** How inner chunks tile a shard, and how its index is read. */
export interface ShardLayout {
  /** Inner chunks per shard along each axis. */
  grid: number[]
  /** Inner chunks per shard in all. */
  count: number
  /** Decodes the index; also states its encoded length. */
  index_pipeline: ReturnType<typeof create_codec_pipeline<'uint64'>>
  /** The index's length before `index_codecs`: 16 bytes per inner chunk. */
  raw_index_size: number
}

export function shard_layout(
  sharding: ShardingInfo,
  chunk_shape: number[],
): ShardLayout {
  const grid = sharding.shard_shape.map((s, i) => s / chunk_shape[i])
  const count = grid.reduce((a, b) => a * b, 1)
  return {
    grid,
    count,
    index_pipeline: create_codec_pipeline({
      data_type: 'uint64',
      shape: [...grid, 2],
      codecs: sharding.index_codecs,
    }),
    raw_index_size: 16 * count,
  }
}

/**
 * Every inner chunk position in a shard, in C order, with its position's
 * flat index — the order inner chunks are laid out in a shard built here.
 */
export function* shard_positions(
  grid: number[],
): IterableIterator<{ local: number[]; flat: number }> {
  const count = grid.reduce((a, b) => a * b, 1)
  for (let flat = 0; flat < count; flat++) {
    const local = new Array<number>(grid.length)
    let rest = flat
    for (let i = grid.length - 1; i >= 0; i--) {
      local[i] = rest % grid[i]
      rest = Math.floor(rest / grid[i])
    }
    yield { local, flat }
  }
}

/**
 * Decode a shard index from its bytes. Copied first: a range or slice may
 * start at any byte, and a uint64 view needs an 8-byte-aligned start.
 */
async function decode_index(
  layout: ShardLayout,
  bytes: Uint8Array,
): Promise<ShardIndex> {
  return (await layout.index_pipeline.decode(bytes.slice())) as unknown as ShardIndex
}

/**
 * The `(offset, length)` of the inner chunk at `local` coordinates within
 * its shard, or `null` when the shard holds no such chunk. Anything a number
 * cannot hold exactly is not a position in a shard; `Number()` would round
 * it rather than refuse it.
 */
export function index_entry(
  index: ShardIndex,
  local: number[],
  shard_path: string,
): { offset: number; length: number } | null {
  const entry = local.reduce((acc, c, i) => acc + c * index.stride[i], 0)
  const offset = index.data[entry]
  const length = index.data[entry + index.stride[index.stride.length - 1]]
  if (offset === MAX_UINT64 && length === MAX_UINT64) return null
  if (offset > MAX_SAFE_UINT64 || length > MAX_SAFE_UINT64) {
    throw new InvalidMetadataError(
      `Shard ${shard_path} index entry for inner chunk [${local}] is out of ` +
        `range: offset ${offset}, length ${length}`,
    )
  }
  return { offset: Number(offset), length: Number(length) }
}

// ---------------------------------------------------------------------------
// Building shards
// ---------------------------------------------------------------------------

/** CRC-32C (Castagnoli) lookup table, built on first use. */
let crc32c_table: Uint32Array | undefined

/**
 * CRC-32C of `bytes`, as the `crc32c` codec appends it. zarrita's codec only
 * strips a checksum on decode; writing a shard index needs to produce one.
 */
export function crc32c(bytes: Uint8Array): number {
  if (!crc32c_table) {
    crc32c_table = new Uint32Array(256)
    for (let n = 0; n < 256; n++) {
      let c = n
      for (let k = 0; k < 8; k++) c = c & 1 ? 0x82f63b78 ^ (c >>> 1) : c >>> 1
      crc32c_table[n] = c
    }
  }
  let crc = 0xffffffff
  for (let i = 0; i < bytes.length; i++) {
    crc = crc32c_table[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8)
  }
  return (crc ^ 0xffffffff) >>> 0
}

/**
 * Encode a shard index with `index_codecs`.
 *
 * Done here rather than through the codec pipeline because zarrita's
 * `crc32c` codec has no encoder, and the index codecs a shard can carry are
 * few: `bytes` (fixed size, either byte order) and `crc32c`, which is what
 * every writer emits. Anything else is refused rather than guessed at.
 */
export function encode_shard_index(
  index: BigUint64Array,
  index_codecs: CodecMetadata[],
): Uint8Array {
  let bytes = new Uint8Array(index.buffer.slice(0))
  for (const codec of index_codecs) {
    if (codec.name === 'bytes') {
      const endian =
        (codec.configuration as { endian?: string } | undefined)?.endian ??
        'little'
      if ((endian === 'big') === system_is_little_endian()) {
        byteswap_inplace(bytes, 8)
      }
    } else if (codec.name === 'crc32c') {
      const checked = new Uint8Array(bytes.length + 4)
      checked.set(bytes)
      new DataView(checked.buffer).setUint32(bytes.length, crc32c(bytes), true)
      bytes = checked
    } else {
      throw new UnsupportedError(`encoding a shard index with codec "${codec.name}"`)
    }
  }
  return bytes
}

/**
 * Assemble a shard from its inner chunks' encoded bytes, given in C order
 * over the shard's inner chunk grid, `undefined` where a chunk is absent.
 */
export function assemble_shard(
  parts: readonly (Uint8Array | undefined)[],
  sharding: ShardingInfo,
): Uint8Array {
  const count = parts.length
  const index = new BigUint64Array(count * 2).fill(MAX_UINT64)
  // Offsets are from the shard's start, so an index at the start pushes the
  // data past itself. Its encoded length is fixed, so encode a blank first.
  const index_size = encode_shard_index(index, sharding.index_codecs).length
  const data_size = parts.reduce((n, part) => n + (part?.length ?? 0), 0)
  const shard = new Uint8Array(data_size + index_size)
  const at_start = sharding.index_location === 'start'
  let at = at_start ? index_size : 0
  parts.forEach((part, i) => {
    if (!part) return
    index[i * 2] = BigInt(at)
    index[i * 2 + 1] = BigInt(part.length)
    shard.set(part, at)
    at += part.length
  })
  shard.set(
    encode_shard_index(index, sharding.index_codecs),
    at_start ? 0 : data_size,
  )
  return shard
}

/** A shard as stored, for rewriting: its bytes and its decoded index. */
export interface ExistingShard {
  bytes: Uint8Array
  index: ShardIndex
}

/**
 * Fetch a whole shard and decode its index, for a rewrite that keeps some of
 * its inner chunks. `null` when the store has no such shard.
 */
export async function read_shard<Store extends Readable>(
  store: Store,
  shard_path: string,
  sharding: ShardingInfo,
  layout: ShardLayout,
  storeOpts?: Parameters<Store['get']>[1],
): Promise<ExistingShard | null> {
  const bytes = await store.get(shard_path as `/${string}`, storeOpts)
  if (!bytes) return null
  const size = await layout.index_pipeline.computeEncodedSize(
    layout.raw_index_size,
  )
  if (bytes.length < size) {
    throw new InvalidMetadataError(
      `Shard ${shard_path} is ${bytes.length} bytes, shorter than its ` +
        `${size}-byte index`,
    )
  }
  const index_bytes =
    sharding.index_location === 'end'
      ? bytes.subarray(bytes.length - size)
      : bytes.subarray(0, size)
  return { bytes, index: await decode_index(layout, index_bytes) }
}

/**
 * The encoded bytes of one inner chunk of a fetched shard — a view, not a
 * copy — or `undefined` when the shard holds no such chunk.
 */
export function inner_chunk_bytes(
  shard: ExistingShard,
  local: number[],
  shard_path: string,
): Uint8Array | undefined {
  const entry = index_entry(shard.index, local, shard_path)
  if (!entry) return undefined
  const { offset, length } = entry
  if (offset + length > shard.bytes.length) {
    throw new InvalidMetadataError(
      `Shard ${shard_path} index entry for inner chunk [${local}] points ` +
        `past the shard's end: offset ${offset}, length ${length}, ` +
        `shard ${shard.bytes.length} bytes`,
    )
  }
  return shard.bytes.subarray(offset, offset + length)
}

// ---------------------------------------------------------------------------
// Chunk sources — reading
// ---------------------------------------------------------------------------

/** Where a chunk's bytes live in the store, and how to fetch them. */
export interface ChunkSource {
  /** The store key the bytes are read from — for a sharded array, the shard's. */
  path: string
  /** The chunk's encoded bytes, or `undefined` when the store has none. */
  fetch(): Promise<Uint8Array | undefined>
}

/**
 * Decoded shard indexes, per store and shard path, for the store's lifetime.
 *
 * An index is a few bytes at the end of a shard, but fetching it is a store
 * round-trip, and every inner chunk of the shard — across every read of the
 * array — needs it. Cached for as long as the store lives, like zarrita
 * caches it for as long as the `Array` lives, and like {@link resolveArrayInfo}
 * memoises the metadata: a shard's index changes only when the shard is
 * rewritten, and a rewrite through `setWorker` drops the entry (see
 * {@link forget_shard_index}). A rejected fetch is evicted, so a transient
 * failure is retried by the next read rather than kept.
 */
const shardIndexes = new WeakMap<object, Map<string, Promise<ShardIndex | null>>>()

/**
 * Drop the remembered index of a shard that has just been rewritten, so the
 * next read fetches the new one instead of using the old offsets on the new
 * bytes.
 */
export function forget_shard_index(store: object, shard_path: string): void {
  shardIndexes.get(store)?.delete(shard_path)
}

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
  const layout = shard_layout(sharding, chunk_shape)
  const { grid } = layout

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
        const size = await layout.index_pipeline.computeEncodedSize(
          layout.raw_index_size,
        )
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
        try {
          return await decode_index(layout, bytes)
        } catch (error) {
          // An index codec that threw: the same structured error an inner
          // chunk's codec would raise, naming the shard.
          throw atChunk(error, shard_path)
        }
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
    const local = chunk_coords.map((c, i) => c % grid[i])
    const path = arr.resolve(encodeShardKey(shard_coords)).path
    return {
      path,
      async fetch() {
        const index = await index_for(path)
        if (index === null) return undefined
        const entry = index_entry(index, local, path)
        if (entry === null) return undefined
        const bytes = await getRange(
          path as `/${string}`,
          entry,
          storeOpts as GetOptions,
        )
        // The shard's size is not known here, but the index has promised a
        // chunk of `entry.length` bytes at `entry.offset`: a range that comes
        // back short, or not at all, is an index that disagrees with its
        // shard — not a chunk to decode, nor a missing one to fill in.
        if (!bytes || bytes.length !== entry.length) {
          throw new InvalidMetadataError(
            `Shard ${path} index entry for inner chunk [${local}] promises ` +
              `${entry.length} bytes at offset ${entry.offset}; the store ` +
              `returned ${bytes ? bytes.length : 'none'}`,
          )
        }
        return bytes
      },
    }
  }
}
