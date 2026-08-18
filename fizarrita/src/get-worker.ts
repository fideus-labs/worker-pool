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

import type {
  WorkerLike,
  WorkerPool,
  WorkerPoolTask,
} from "@fideus-labs/worker-pool"
import type {
  Chunk,
  CodecMetadata,
  DataType,
  Readable,
  Scalar,
  Slice,
  Array as ZarrArray,
} from "zarrita"

import { createCodecWorker } from "./create-worker.js"
import { create_codec_pipeline } from "./internals/codec-pipeline.js"
import { BasicIndexer } from "./internals/indexer.js"
import { setter } from "./internals/setter.js"
import {
  assertSharedArrayBufferAvailable,
  create_chunk_key_encoder,
  createBuffer,
  get_ctr,
  get_strides,
} from "./internals/util.js"
import type { ChunkCache, CodecChunkMeta, GetWorkerOptions } from "./types.js"
import { getMetaId, workerDecode, workerDecodeInto } from "./worker-rpc.js"

/** Shared TextDecoder instance. */
const decoder = new TextDecoder()

// ---------------------------------------------------------------------------
// Chunk cache helpers — store-scoped key generation
// ---------------------------------------------------------------------------

/** No-op cache used when the caller doesn't provide one. */
const NULL_CACHE: ChunkCache = {
  get: () => undefined,
  set: () => {},
}

/** WeakMap to assign unique IDs to store instances, preventing cache collisions. */
const storeIdMap = new WeakMap<object, number>()
let storeIdCounter = 0

export function getStoreId(store: Readable): string {
  if (!storeIdMap.has(store)) {
    storeIdMap.set(store, storeIdCounter++)
  }
  return `store_${storeIdMap.get(store)}`
}

export function createCacheKey<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
  encodeChunkKey: (chunk_coords: number[]) => string,
  chunk_coords: number[],
): string {
  const chunkKey = encodeChunkKey(chunk_coords)
  const storeId = getStoreId(arr.store)
  return `${storeId}:${arr.path}:${chunkKey}`
}

/**
 * Chunk fetch+decode operations currently in flight, keyed exactly like the
 * cache, so that concurrent readers of the same chunk share one of each.
 *
 * A cache alone cannot do this: `ChunkCache` is synchronous and holds decoded
 * chunks, so nothing lands in it until a decode has already finished. Two
 * `getWorker` calls that overlap — two viewports, a re-render arriving mid-flight
 * — therefore both miss, both fetch, and both decode the very same bytes. This
 * map is what closes the window between "someone started fetching this" and
 * "the result is cacheable".
 */
interface PendingChunk {
  promise: Promise<Chunk<DataType>>
  /**
   * The abort signal the producing fetch runs under, if any. A follower whose
   * shared promise rejects checks this to tell "the producer's caller walked
   * away" apart from a real failure — precisely, rather than by sniffing the
   * rejection's shape, which a custom abort reason would defeat.
   */
  signal?: AbortSignal
}

const pendingChunks = new Map<string, PendingChunk>()

/**
 * Run `produce` once per key, handing concurrent callers the same promise.
 *
 * The entry is removed as soon as it settles, on both paths: keeping a rejection
 * would make one transient fetch failure permanent for that chunk, and keeping a
 * fulfilment would duplicate the cache while pinning chunks the cache has since
 * evicted. Removal is conditional on the entry still being ours so a later
 * attempt that already replaced it is not dropped by our own settlement.
 *
 * Alongside the promise the caller gets `producerSignal` — the signal the
 * producing fetch runs under, its own `signal` when it became the producer.
 * The pair is what lets a follower retry a chunk whose producer aborted.
 */
function shareInFlightChunk<D extends DataType>(
  key: string,
  produce: () => Promise<Chunk<D>>,
  signal?: AbortSignal,
): { promise: Promise<Chunk<D>>; producerSignal?: AbortSignal } {
  const inFlight = pendingChunks.get(key)
  if (inFlight) {
    return {
      promise: inFlight.promise as Promise<Chunk<D>>,
      producerSignal: inFlight.signal,
    }
  }

  const promise = produce()
  const entry: PendingChunk = {
    promise: promise as unknown as Promise<Chunk<DataType>>,
    signal,
  }
  pendingChunks.set(key, entry)

  const forget = () => {
    if (pendingChunks.get(key) === entry) {
      pendingChunks.delete(key)
    }
  }
  // Both handlers swallow: this branch exists only to clean up, and the caller
  // still receives `promise` itself and still sees the rejection.
  promise.then(forget, forget)

  return { promise, producerSignal: signal }
}

// ---------------------------------------------------------------------------
// Unified metadata reader — reads zarr.json once, returns everything needed
// ---------------------------------------------------------------------------

export interface ArrayMetadata {
  codecMeta: CodecChunkMeta
  encodeChunkKey: (chunk_coords: number[]) => string
  fillValue: Scalar<DataType> | null
}

/**
 * Read a zarr array's metadata, trying v3 (`zarr.json`) then v2 (`.zarray`).
 *
 * `storeOpts` is forwarded to every `store.get` this makes, so an AbortSignal,
 * auth header, or any other per-request option governs the metadata reads on
 * the same terms as the chunk reads that follow them — a signal that aborts
 * the chunk fetches but silently leaves the `zarr.json` read running would be
 * a surprising asymmetry.
 */
export async function readArrayMetadata<
  D extends DataType,
  Store extends Readable,
>(
  arr: ZarrArray<D, Store>,
  storeOpts?: Parameters<Store["get"]>[1],
): Promise<ArrayMetadata> {
  const store = arr.store

  // Try v3 first: read zarr.json
  const v3Path = (
    arr.path === "/" ? "/zarr.json" : `${arr.path}/zarr.json`
  ) as `/${string}`
  const v3Bytes = await store.get(v3Path, storeOpts)
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
  const v2Path = (
    arr.path === "/" ? "/.zarray" : `${arr.path}/.zarray`
  ) as `/${string}`
  const v2Bytes = await store.get(v2Path, storeOpts)
  if (v2Bytes) {
    const metadata = JSON.parse(decoder.decode(v2Bytes))
    const codecs: Array<{
      name: string
      configuration: Record<string, unknown>
    }> = []
    if (metadata.order === "F") {
      codecs.push({ name: "transpose", configuration: { order: "F" } })
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
        codecs:
          codecs.length > 0
            ? codecs
            : [{ name: "bytes", configuration: { endian: "little" } }],
      },
      encodeChunkKey: create_chunk_key_encoder({
        name: "v2",
        configuration: { separator: metadata.dimension_separator ?? "." },
      }),
      fillValue: metadata.fill_value ?? null,
    }
  }

  // Fallback: BytesCodec only, default v3 key encoding
  return {
    codecMeta: {
      data_type: arr.dtype,
      chunk_shape: arr.chunks,
      codecs: [{ name: "bytes", configuration: { endian: "little" } }],
    },
    encodeChunkKey: create_chunk_key_encoder({ name: "default" }),
    fillValue: null,
  }
}

// ---------------------------------------------------------------------------
// Chunk shape probing — detect and correct wrong metadata chunk_shape
// ---------------------------------------------------------------------------

/**
 * Read the decompressed (frame content) size from a zstd-compressed buffer's
 * frame header, without decompressing. Returns null if not zstd or if the
 * frame content size is not present.
 *
 * Zstd frame format:
 *   [4 bytes magic 0xFD2FB528] [1 byte FHD] [0-1 byte window] [0-4 dict] [0-8 FCS]
 */
export function readZstdFrameContentSize(
  compressed: Uint8Array,
): number | null {
  if (compressed.length < 6) return null

  const magic =
    compressed[0] |
    (compressed[1] << 8) |
    (compressed[2] << 16) |
    (compressed[3] << 24)
  if (magic >>> 0 !== 0xfd2fb528) return null

  const fhd = compressed[4]
  const fcsFlag = (fhd >> 6) & 3
  const singleSegment = (fhd >> 5) & 1
  const dictIdFlag = fhd & 3
  const dictIdSize = [0, 1, 2, 4][dictIdFlag]
  const windowDescSize = singleSegment ? 0 : 1

  let fcsFieldSize: number
  if (fcsFlag === 0) fcsFieldSize = singleSegment ? 1 : 0
  else if (fcsFlag === 1) fcsFieldSize = 2
  else if (fcsFlag === 2) fcsFieldSize = 4
  else fcsFieldSize = 8

  if (fcsFieldSize === 0) return null

  const offset = 5 + windowDescSize + dictIdSize
  if (compressed.length < offset + fcsFieldSize) return null

  if (fcsFieldSize === 1) return compressed[offset]
  if (fcsFieldSize === 2) {
    return (compressed[offset] | (compressed[offset + 1] << 8)) + 256
  }
  if (fcsFieldSize === 4) {
    return (
      (compressed[offset] |
        (compressed[offset + 1] << 8) |
        (compressed[offset + 2] << 16) |
        (compressed[offset + 3] << 24)) >>>
      0
    )
  }
  // 8-byte: use DataView for 64-bit (return as Number, safe for chunk sizes)
  const dv = new DataView(compressed.buffer, compressed.byteOffset + offset, 8)
  return Number(dv.getBigUint64(0, true))
}

/**
 * Read the uncompressed size (nbytes) from a blosc-compressed buffer's header,
 * without decompressing. Returns null if not a valid blosc buffer.
 *
 * Blosc 1.x header (16 bytes, little-endian):
 *   [1 byte version] [1 byte versionlz] [1 byte flags] [1 byte typesize]
 *   [4 bytes nbytes] [4 bytes blocksize] [4 bytes cbytes]
 *
 * The nbytes field at offset 4 is the uncompressed data size in bytes.
 */
export function readBloscFrameContentSize(
  compressed: Uint8Array,
): number | null {
  if (compressed.length < 16) return null

  // Blosc version must be >= 1 (version byte at offset 0)
  const version = compressed[0]
  if (version < 1 || version > 2) return null

  // Sanity: typesize at offset 3 should be 1-8 for typical numeric data
  const typesize = compressed[3]
  if (typesize === 0 || typesize > 8) return null

  // Read nbytes (uint32 LE) at offset 4
  const nbytes =
    (compressed[4] |
      (compressed[5] << 8) |
      (compressed[6] << 16) |
      (compressed[7] << 24)) >>>
    0

  // Read cbytes (uint32 LE) at offset 12
  const cbytes =
    (compressed[12] |
      (compressed[13] << 8) |
      (compressed[14] << 16) |
      (compressed[15] << 24)) >>>
    0

  // Sanity: cbytes should roughly match the actual buffer size
  // Allow some slack since the buffer might contain trailing data
  if (cbytes === 0 || cbytes > compressed.length + 16) return null

  // Sanity: nbytes should be reasonable (> 0, not astronomically large)
  if (nbytes === 0) return null

  return nbytes
}

/**
 * Codecs whose encoded output is exactly as long as the array bytes they encode,
 * so that a raw chunk's `byteLength` IS its decoded byte length.
 *
 * The list is short because the property is strict. `transpose` reorders and
 * `bytes` reinterprets, neither changing the count. Everything else changes it
 * one way or another: compressors shrink, `crc32c` appends a checksum,
 * `scale_offset` and `cast_value` re-type the values, `vlen-utf8` and `json2`
 * are variable-length, and `sharding_indexed` wraps a whole index plus inner
 * chunks that are usually compressed themselves.
 *
 * v2-style `numcodecs.` prefixes are stripped before lookup, so
 * `numcodecs.transpose` matches.
 */
const SIZE_PRESERVING_CODECS = new Set(["bytes", "transpose"])

/**
 * Whether any codec in the chain makes the raw chunk length differ from the
 * decoded length.
 *
 * Deliberately answered by allowlisting the codecs known to preserve size, not
 * by listing the compressors: an unrecognised codec must count as size-changing.
 * The two mistakes are not symmetric — treating a size-changing codec as
 * size-preserving returns the *compressed* length as the decompressed one, a
 * silently wrong number that {@link inferChunkShape} then takes as fact, while
 * treating a size-preserving codec as size-changing only costs one decode via
 * the fallback and still returns the right answer.
 *
 * Naming compressors instead put every codec outside a fixed list —
 * e.g. HTJ2K — on the silently-wrong side.
 */
export function hasSizeChangingCodec(
  codecs: readonly Pick<CodecMetadata, "name">[],
): boolean {
  return codecs.some((codec) => {
    const name = codec.name.toLowerCase().replace(/^numcodecs\./, "")
    return !SIZE_PRESERVING_CODECS.has(name)
  })
}

/**
 * Try to determine the decompressed byte size of a raw chunk without full decoding.
 *
 * Hybrid strategy (cheapest first):
 *  1. Zstd frame header — read FCS field (zero-cost, no decompression)
 *  2. Blosc header — read nbytes field (zero-cost, no decompression)
 *  3. Size-preserving check — if every codec in the chain preserves byte count,
 *     the raw byte count IS the decompressed size
 *  4. Full decode — decode chunk c/0/0/0 using the codec pipeline and count elements
 *
 * Returns the decompressed byte size, or null if detection failed.
 */
async function probeDecompressedSize<D extends DataType>(
  rawBytes: Uint8Array,
  codecMeta: CodecChunkMeta,
  bytesPerElement: number,
): Promise<number | null> {
  // 1. Try zstd header (cheapest — just reads a few bytes)
  const zstdSize = readZstdFrameContentSize(rawBytes)
  if (zstdSize != null) return zstdSize

  // 2. Try blosc header
  const bloscSize = readBloscFrameContentSize(rawBytes)
  if (bloscSize != null) return bloscSize

  // 3. Check whether the codec chain preserves byte count end to end — a
  //    transpose + bytes chain does, not just a bare bytes one. When it does,
  //    rawBytes.byteLength IS the decompressed size.
  if (!hasSizeChangingCodec(codecMeta.codecs)) {
    return rawBytes.byteLength
  }

  // 4. Full decode fallback — decode the chunk using the codec pipeline
  //    This handles any codec (gzip, lz4, etc.) at the cost of one decompression
  try {
    const pipeline = create_codec_pipeline({
      data_type: codecMeta.data_type,
      shape: codecMeta.chunk_shape,
      codecs: codecMeta.codecs,
    })
    const chunk = await pipeline.decode(rawBytes)
    const data = chunk.data as unknown as ArrayLike<unknown>
    return data.length * bytesPerElement
  } catch {
    return null
  }
}

interface ChunkShapeCandidate {
  shape: number[]
  score: number
}

/**
 * Infer candidate chunk shapes from the decompressed element count.
 * Returns an array of candidates sorted by quality score (lower = better).
 *
 * Scoring considers:
 *  1. Closeness to metadata chunk_shape (L1 distance)
 *  2. Whether each chunk dimension is a power-of-2 (common in scientific imaging)
 *  3. Whether the chunk dimensions evenly divide the array shape
 *  4. Whether chunk_x >= chunk_y (OME-Zarr convention for faster-varying dims)
 */
export function inferChunkShape(
  actualElements: number,
  metadataChunkShape: number[],
  arrayShape: number[],
): number[][] {
  const ndim = metadataChunkShape.length
  const metaElements = metadataChunkShape.reduce((a, b) => a * b, 1)
  if (actualElements === metaElements) return [metadataChunkShape]

  const allCandidates: ChunkShapeCandidate[] = []
  const seen = new Set<string>()

  function isPowerOf2(n: number): boolean {
    return n > 0 && (n & (n - 1)) === 0
  }

  function scoreCandidate(shape: number[]): number {
    // L1 distance from metadata
    let l1 = 0
    for (let i = 0; i < shape.length; i++)
      l1 += Math.abs(shape[i] - metadataChunkShape[i])

    // Penalty for non-power-of-2 dimensions
    let pow2Penalty = 0
    for (let i = 0; i < shape.length; i++) {
      if (!isPowerOf2(shape[i])) pow2Penalty += 10
    }

    // Penalty for not evenly dividing array shape
    let divPenalty = 0
    for (let i = 0; i < shape.length; i++) {
      if (arrayShape[i] % shape[i] !== 0) divPenalty += 5
    }

    // Penalty for chunk dim > array dim (invalid)
    let overPenalty = 0
    for (let i = 0; i < shape.length; i++) {
      if (shape[i] > arrayShape[i]) overPenalty += 1000
    }

    // OME-Zarr convention: for 3D (z,y,x), prefer chunk_x >= chunk_y
    let conventionPenalty = 0
    if (ndim >= 2) {
      const lastDim = shape[ndim - 1] // x
      const prevDim = shape[ndim - 2] // y
      if (lastDim < prevDim) conventionPenalty += 20
    }

    return l1 + pow2Penalty + divPenalty + overPenalty + conventionPenalty
  }

  function addCandidate(shape: number[]): void {
    const key = shape.join(",")
    if (seen.has(key)) return
    seen.add(key)
    allCandidates.push({ shape, score: scoreCandidate(shape) })
  }

  // Strategy 1: Keep all but one dimension from metadata, solve for the remaining
  for (let vary = 0; vary < ndim; vary++) {
    const fixedProduct = metadataChunkShape.reduce(
      (acc, v, i) => (i === vary ? acc : acc * v),
      1,
    )
    if (fixedProduct === 0 || actualElements % fixedProduct !== 0) continue
    const candidate = actualElements / fixedProduct
    if (
      candidate > 0 &&
      candidate <= arrayShape[vary] &&
      Number.isInteger(candidate)
    ) {
      const result = [...metadataChunkShape]
      result[vary] = candidate
      addCandidate(result)
    }
  }

  // Strategy 2: For 3D arrays, try common chunk sizes for two dimensions, solve for third
  if (ndim === 3) {
    const commonSizes = [256, 128, 96, 64, 48, 32]
    for (const cy of commonSizes) {
      if (cy > arrayShape[1] || actualElements % cy !== 0) continue
      for (const cx of commonSizes) {
        if (cx > arrayShape[2]) continue
        const yx = cy * cx
        if (actualElements % yx !== 0) continue
        const cz = actualElements / yx
        if (cz > 0 && cz <= arrayShape[0] && Number.isInteger(cz)) {
          addCandidate([cz, cy, cx])
        }
      }
    }
  }

  // Strategy 3: Try adjusting one dim by small delta, solve for other two
  if (ndim === 3) {
    const deltas = [1, -1, 2, -2, 3, -3]
    const commonSizes = [256, 128, 96, 64, 48, 32]
    for (let vary = 0; vary < ndim; vary++) {
      for (const delta of deltas) {
        const trial = metadataChunkShape[vary] + delta
        if (trial <= 0 || trial > arrayShape[vary]) continue
        if (actualElements % trial !== 0) continue
        const remaining = actualElements / trial
        const otherDims: number[] = []
        for (let d = 0; d < ndim; d++) {
          if (d !== vary) otherDims.push(d)
        }
        const [d1, d2] = otherDims
        for (const s1 of commonSizes) {
          if (s1 > arrayShape[d1] || remaining % s1 !== 0) continue
          const s2 = remaining / s1
          if (s2 > 0 && s2 <= arrayShape[d2] && Number.isInteger(s2)) {
            const result = [...metadataChunkShape]
            result[vary] = trial
            result[d1] = s1
            result[d2] = s2
            addCandidate(result)
          }
        }
      }
    }
  }

  // Sort by quality score (lower = better)
  allCandidates.sort((a, b) => a.score - b.score)
  return allCandidates.map((c) => c.shape)
}

/**
 * Validate a candidate chunk shape by probing one-past-the-end along the
 * dimension with the smallest grid extent. If the probe returns data, the
 * candidate's chunks are too large (the real grid has more chunks in that
 * dimension) and should be rejected.
 *
 * `valid` is true if the candidate holds (probe returned 404/empty), false if
 * invalid (probe returned data, meaning chunks are too coarse). `conclusive`
 * is false when the answer came from a swallowed fetch error rather than from
 * a completed probe — see {@link ChunkShapeProbe}.
 */
async function validateCandidateChunkShape<
  D extends DataType,
  Store extends Readable,
>(
  arr: ZarrArray<D, Store>,
  encodeChunkKey: (chunk_coords: number[]) => string,
  candidate: number[],
  storeOpts?: Parameters<Store["get"]>[1],
  signal?: AbortSignal,
): Promise<{ valid: boolean; conclusive: boolean }> {
  const ndim = candidate.length

  // Compute grid dimensions and find the dimension with the smallest extent > 1
  // (most likely to differ between wrong and correct candidates)
  const gridDims = candidate.map((c, i) => Math.ceil(arr.shape[i] / c))

  // Find a dimension where gridDims > 1 to probe one-past-the-end
  // Prefer the dimension with the smallest grid extent (fewest chunks),
  // as that's where over-sized chunks are most detectable
  let probeDim = -1
  let minGrid = Infinity
  for (let i = 0; i < ndim; i++) {
    if (gridDims[i] > 1 && gridDims[i] < minGrid) {
      minGrid = gridDims[i]
      probeDim = i
    }
  }

  if (probeDim === -1) {
    // All dimensions have only 1 chunk — can't validate, assume correct.
    // Conclusive: this answer is a property of the grid, not of a failed
    // request, so it will be the same on every retry.
    return { valid: true, conclusive: true }
  }

  // Probe one-past-the-end: if the store has a chunk at this coordinate,
  // the candidate's chunks are too large (real grid is finer)
  const probeCoords = candidate.map(() => 0)
  probeCoords[probeDim] = gridDims[probeDim] // one past last valid index
  const probeKey = encodeChunkKey(probeCoords)
  const probePath = arr.resolve(probeKey).path

  try {
    const probeBytes = await arr.store.get(probePath, storeOpts)
    // If data returned, there's a chunk beyond our expected grid → reject
    return { valid: !probeBytes, conclusive: true }
  } catch (error) {
    // A caller abort is not a probe outcome — the whole read is over.
    if (signal?.aborted) throw error
    // Fetch error (404, network error) → no chunk there → accept. The two are
    // indistinguishable here, so the acceptance is a guess made under an
    // error and must not be memoised as settled.
    return { valid: true, conclusive: false }
  }
}

/**
 * Detect actual chunk shape by probing the first chunk's decompressed size
 * and using heuristic scoring to infer the most likely chunk shape.
 *
 * Uses a hybrid approach to determine decompressed size:
 *  1. Zstd frame header (zero-cost)
 *  2. Blosc header (zero-cost)
 *  3. Raw size check for uncompressed data (zero-cost)
 *  4. Full decode fallback for any other codec (one decompression)
 *
 * After inference, validates the top candidate by probing one-past-the-end
 * along its smallest grid dimension. If the store has a chunk beyond the
 * candidate's expected grid, the candidate is rejected in favor of the next.
 *
 * Returns the corrected chunk shape, or the metadata chunk shape if no
 * correction is needed or possible.
 */
export async function probeActualChunkShape<
  D extends DataType,
  Store extends Readable,
>(
  arr: ZarrArray<D, Store>,
  encodeChunkKey: (chunk_coords: number[]) => string,
  codecMeta: CodecChunkMeta,
  bytesPerElement: number,
  storeOpts?: Parameters<Store["get"]>[1],
  signal?: AbortSignal,
): Promise<number[]> {
  const { shape } = await probeChunkShape(
    arr,
    encodeChunkKey,
    codecMeta,
    bytesPerElement,
    storeOpts,
    signal,
  )
  return shape
}

/**
 * A probed chunk shape, plus whether the probe actually concluded it.
 *
 * `conclusive` is false when the shape is what the probe fell back to after
 * swallowing a store failure — a fetch that threw — rather than what it read
 * from the data. (An abort of the `signal` the probe was given to watch is not
 * swallowed at all: it propagates and ends the read.)
 *
 * The distinction exists because {@link resolveArrayInfo} memoises the result.
 * Swallowing the failure is right for a single read: the probe is a heuristic
 * correction, and failing a whole read because a *heuristic* could not fetch
 * `c/0/0` would break reads of arrays whose first chunk merely happens to be
 * unreachable. But an inconclusive answer must not outlive the call that made
 * it. Before memoisation each read re-probed, so a transient blip cost one
 * uncorrected read and healed itself; cached forever, that same blip leaves a
 * mis-declared array decoding at the wrong shape for the lifetime of the
 * store. So the fallback still returns — and is then refused a cache entry.
 */
interface ChunkShapeProbe {
  shape: number[]
  conclusive: boolean
}

/**
 * {@link probeActualChunkShape}, reporting whether the answer was concluded
 * from data or fallen back to after a store failure.
 */
async function probeChunkShape<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
  encodeChunkKey: (chunk_coords: number[]) => string,
  codecMeta: CodecChunkMeta,
  bytesPerElement: number,
  storeOpts?: Parameters<Store["get"]>[1],
  signal?: AbortSignal,
): Promise<ChunkShapeProbe> {
  const metadataChunkShape = codecMeta.chunk_shape
  const metaElements = metadataChunkShape.reduce((a, b) => a * b, 1)

  // Fetch the first chunk (c/0/0/0)
  const zeroCoords = metadataChunkShape.map(() => 0)
  const chunkKey = encodeChunkKey(zeroCoords)
  const chunkPath = arr.resolve(chunkKey).path

  // Every early return below is conclusive: each is a determination made from
  // bytes actually read, so re-probing would reach the same answer.
  try {
    const rawBytes = await arr.store.get(chunkPath, storeOpts)
    if (!rawBytes) return { shape: metadataChunkShape, conclusive: true }

    // Determine decompressed size via hybrid strategy
    const decompressedBytes = await probeDecompressedSize(
      rawBytes,
      codecMeta,
      bytesPerElement,
    )
    if (decompressedBytes == null) {
      return { shape: metadataChunkShape, conclusive: true }
    }

    const actualElements = decompressedBytes / bytesPerElement
    if (actualElements === metaElements) {
      return { shape: metadataChunkShape, conclusive: true }
    }

    // Mismatch detected — infer chunk shape from element count + heuristics
    const candidates = inferChunkShape(
      actualElements,
      metadataChunkShape,
      arr.shape,
    )
    if (candidates.length === 0) {
      return { shape: metadataChunkShape, conclusive: true }
    }

    // Validate candidates by probing one-past-the-end.
    // The first candidate that passes validation wins.
    // Limit validation attempts to avoid excessive network requests.
    // A candidate accepted because its validation probe *failed* rather than
    // came back empty taints the result: the choice was a guess, so it is
    // returned but not treated as settled.
    let conclusive = true
    const maxValidationAttempts = Math.min(candidates.length, 5)
    for (let i = 0; i < maxValidationAttempts; i++) {
      const candidate = candidates[i]
      const validation = await validateCandidateChunkShape(
        arr,
        encodeChunkKey,
        candidate,
        storeOpts,
        signal,
      )
      if (!validation.conclusive) conclusive = false
      if (validation.valid) {
        console.warn(
          `[fizarrita] Metadata chunk_shape ${JSON.stringify(metadataChunkShape)} ` +
            `does not match actual chunk data (${actualElements} elements). ` +
            `Using inferred chunk_shape: ${JSON.stringify(candidate)}`,
        )
        return { shape: candidate, conclusive }
      }
    }

    // No candidate passed validation — fall back to best-scored
    const fallback = candidates[0]
    console.warn(
      `[fizarrita] Metadata chunk_shape ${JSON.stringify(metadataChunkShape)} ` +
        `does not match actual chunk data (${actualElements} elements). ` +
        `Using inferred chunk_shape: ${JSON.stringify(fallback)} (unvalidated)`,
    )
    return { shape: fallback, conclusive }
  } catch (error) {
    // The catch-all exists to degrade gracefully when the probe fetch fails;
    // a caller abort is not that — it has to stop the whole read.
    if (signal?.aborted) throw error
    // A store failure, not a determination — see ChunkShapeProbe.
    return { shape: metadataChunkShape, conclusive: false }
  }
}

// ---------------------------------------------------------------------------
// Per-array resolution — metadata read + chunk-shape probe, memoised
// ---------------------------------------------------------------------------

/**
 * Resolved array info per store, keyed by array path.
 *
 * Both the array metadata and the probed chunk shape are immutable for the
 * lifetime of an array, but resolving them costs store round-trips: one read
 * of `zarr.json` (two, when falling back to v2), one chunk read for the probe,
 * and up to five one-past-the-end probes on a mismatch. Without memoisation
 * every `getWorker` call pays them *before* the chunk cache is consulted, so a
 * fully populated cache cannot eliminate them — for a tiled viewer that is
 * per-tile overhead scaling with pan/zoom activity rather than with cache
 * misses.
 *
 * Keyed on the store instance (a WeakMap, so entries die with the store) plus
 * the array path, mirroring the chunk-cache key of {@link createCacheKey}, so
 * distinct `zarr.open` handles onto the same array share one entry.
 *
 * The promise is memoised, not the value, so concurrent `getWorker` calls on a
 * cold array share one resolution instead of racing store reads.
 */
const resolvedArrayInfo = new WeakMap<
  object,
  Map<string, Promise<{ info: ArrayMetadata; conclusive: boolean }>>
>()

function isAbortSignal(value: unknown): value is AbortSignal {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as AbortSignal).aborted === "boolean" &&
    typeof (value as AbortSignal).addEventListener === "function"
  )
}

/**
 * Split a caller's store options into what a *shared* store request may carry
 * and the caller's own `AbortSignal`, if the options hold one.
 *
 * Everything else — headers, credentials, cache mode — describes how to talk
 * to the store and is the same for every caller of the same store, so it can
 * safely govern a request made on behalf of all of them. A signal is the one
 * option that belongs to a single caller: it says "I no longer want this",
 * which is not a statement the others have made.
 *
 * Only a real signal is separated. Options with no `signal`, or one that is
 * not an `AbortSignal`, are passed through untouched — same object, no copy.
 */
function separateSignal<Opts>(storeOpts: Opts): {
  shared: Opts
  signal: AbortSignal | undefined
} {
  if (storeOpts && typeof storeOpts === "object" && "signal" in storeOpts) {
    const { signal, ...shared } = storeOpts as Opts & { signal?: unknown }
    if (isAbortSignal(signal)) {
      return { shared: shared as Opts, signal }
    }
  }
  return { shared: storeOpts, signal: undefined }
}

/**
 * Settle as `promise` does, unless `signal` aborts first — then reject with
 * the abort reason, exactly as a fetch given that signal would. `promise`
 * itself is untouched and keeps running for whoever else awaits it.
 */
function untilAborted<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T> {
  if (!signal) return promise
  const reason = () =>
    signal.reason ?? new DOMException("The operation was aborted.", "AbortError")
  if (signal.aborted) return Promise.reject(reason())
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(reason())
    signal.addEventListener("abort", onAbort, { once: true })
    const settled = () => signal.removeEventListener("abort", onAbort)
    promise.then(
      (value) => {
        settled()
        resolve(value)
      },
      (error) => {
        settled()
        reject(error)
      },
    )
  })
}

/**
 * A private copy of a memoised entry for one caller. `codecMeta` is plain
 * JSON data (that is what {@link getMetaId} relies on), so a structured clone
 * is a faithful deep copy; the key encoder is a stateless closure and is
 * shared. Nothing the caller does to the copy can reach the memo, or the next
 * caller — and nothing the memo holds is anyone else's object either: the
 * entry itself is built from a clone (see {@link resolveArrayInfo}), because
 * the v2 metadata path hands back zarrita's own `arr.chunks` array by
 * reference.
 */
function detach(info: ArrayMetadata): ArrayMetadata {
  return {
    codecMeta: structuredClone(info.codecMeta),
    encodeChunkKey: info.encodeChunkKey,
    fillValue: structuredClone(info.fillValue),
  }
}

/**
 * Read array metadata and probe the actual chunk shape, once per
 * (store, array path) — repeat calls return the memoised promise without
 * touching the store.
 *
 * The returned metadata's `codecMeta.chunk_shape` already carries the probe's
 * correction, so it describes the chunks as stored, not as the metadata
 * claimed. Each caller receives its own copy; the memoised entry is private
 * to this module and cannot be reached, or altered, through a returned value.
 *
 * `storeOpts` is forwarded to every store read the resolution makes — both the
 * metadata reads and the shape probe — with one exception: an `AbortSignal` in
 * the options is not. The resolution is a shared, memoised resource whose
 * result outlives every caller that wanted it, so it runs on the options that
 * are the same for all of them (headers, credentials, and so on) and cannot be
 * cancelled by any one of them. A caller's signal governs *its own wait*
 * instead: aborting rejects that caller promptly with the signal's reason,
 * while the callers sharing the resolution — and the memo — still get their
 * result. Binding the shared request to whichever caller happened to start it
 * would let one aborted tile fail every other tile that joined it, or hand
 * them the fallback chunk shape for a probe *they* never aborted. The cost is
 * that a resolution nobody wants any more still completes; since the next
 * caller on the array is served from it, that is rarely wasted.
 *
 * Two outcomes are deliberately *not* kept. A rejected resolution is evicted,
 * so a transient store failure is retried by the next call instead of becoming
 * permanent. So is a resolution whose chunk-shape probe was inconclusive — one
 * that swallowed a store failure and fell back to the declared shape rather
 * than reading the real one (see {@link ChunkShapeProbe}). Both still serve the
 * call that produced them, and every caller already waiting on them; they just
 * do not outlive it. Caching a guess made under an error is how a one-off blip
 * would otherwise turn into an array that decodes at the wrong shape forever.
 */
export function resolveArrayInfo<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
  storeOpts?: Parameters<Store["get"]>[1],
): Promise<ArrayMetadata> {
  const { shared, signal } = separateSignal(storeOpts)

  let infoByPath = resolvedArrayInfo.get(arr.store)
  if (!infoByPath) {
    infoByPath = new Map()
    resolvedArrayInfo.set(arr.store, infoByPath)
  }
  const memoised = infoByPath.get(arr.path)
  if (memoised) {
    return untilAborted(
      memoised.then(({ info }) => detach(info)),
      signal,
    )
  }

  const promise = (async () => {
    const { codecMeta, encodeChunkKey, fillValue } = await readArrayMetadata(
      arr,
      shared,
    )
    const Ctr = get_ctr(arr.dtype)
    const bytesPerElement = (Ctr as unknown as { BYTES_PER_ELEMENT: number })
      .BYTES_PER_ELEMENT
    // No signal for the probe to watch: its fetches run under `shared`, which
    // carries none — the caller's signal governs the caller's wait (below),
    // never the shared resolution.
    const { shape, conclusive } = await probeChunkShape(
      arr,
      encodeChunkKey,
      codecMeta,
      bytesPerElement,
      shared,
    )
    return {
      // Cloned so the memo owns its data outright — see detach.
      info: detach({
        codecMeta:
          shape !== codecMeta.chunk_shape
            ? { ...codecMeta, chunk_shape: shape }
            : codecMeta,
        encodeChunkKey,
        fillValue,
      }),
      conclusive,
    }
  })()

  const paths = infoByPath
  paths.set(arr.path, promise)
  // Conditional on the entry still being ours, so a later attempt that already
  // replaced it is not dropped by our own settlement.
  const forget = () => {
    if (paths.get(arr.path) === promise) {
      paths.delete(arr.path)
    }
  }
  promise.then(({ conclusive }) => {
    if (!conclusive) forget()
  }, forget)

  return untilAborted(
    promise.then(({ info }) => detach(info)),
    signal,
  )
}

// ---------------------------------------------------------------------------
// getWorker
// ---------------------------------------------------------------------------

/**
 * Combine two abort signals into one that fires when either does.
 *
 * `AbortSignal.any` where available; on runtimes that predate it (Safari
 * before 17.4, Node before 20.3) a controller bridge. The bridge's listeners
 * stay on the parent signals for the parents' lifetime — acceptable for
 * one-shot read signals, which is the only way this module uses them.
 */
function combineAbortSignals(a: AbortSignal, b: AbortSignal): AbortSignal {
  if (typeof AbortSignal.any === "function") {
    return AbortSignal.any([a, b])
  }
  if (a.aborted) return a
  if (b.aborted) return b
  const controller = new AbortController()
  const forward = (signal: AbortSignal) => {
    signal.addEventListener("abort", () => controller.abort(signal.reason), {
      once: true,
    })
  }
  forward(a)
  forward(b)
  return controller.signal
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
 * @param opts      - Options including the WorkerPool and store options.
 * @returns The result chunk, or a scalar if all dimensions are integer-indexed.
 *
 * @example
 * ```ts
 * import { WorkerPool } from '@fideus-labs/worker-pool'
 * import { getWorker } from '@fideus-labs/fizarrita'
 * import * as zarr from 'zarrita'
 *
 * const pool = new WorkerPool(4)
 * const store = new zarr.FetchStore('https://example.com/data.zarr')
 * const arr = await zarr.open(store, { kind: 'array' })
 *
 * try {
 *   const result = await getWorker(arr, null, { pool })
 * } finally {
 *   pool.terminateWorkers()
 * }
 * ```
 */
export async function getWorker<
  D extends DataType,
  Store extends Readable,
  Sel extends (null | Slice | number)[],
>(
  arr: ZarrArray<D, Store>,
  selection: Sel | null = null,
  opts: GetWorkerOptions<Parameters<Store["get"]>[1]>,
): Promise<
  null extends Sel[number]
    ? Chunk<D>
    : Slice extends Sel[number]
      ? Chunk<D>
      : Scalar<D>
> {
  const { pool, workerUrl, signal } = opts
  const useShared = !!opts.useSharedArrayBuffer
  const cache = opts.cache ?? NULL_CACHE

  // Not `throwIfAborted()`: some runtimes grew `AbortSignal` before that
  // method, and on them the call itself would throw a TypeError on every
  // signalled read, aborted or not.
  if (signal?.aborted) {
    throw signal.reason
  }

  // The signal actually handed to `store.get`: the caller may already carry a
  // store-level signal inside `opts.opts`, and folding ours in must not
  // silently disconnect it — either firing aborts the fetch.
  const storeSignal = (opts.opts as { signal?: AbortSignal } | undefined)
    ?.signal
  const fetchSignal =
    signal && storeSignal
      ? combineAbortSignals(signal, storeSignal)
      : (signal ?? storeSignal)
  const storeOpts =
    fetchSignal === storeSignal
      ? opts.opts
      : ({ ...(opts.opts as object), signal: fetchSignal } as typeof opts.opts)

  if (useShared) {
    assertSharedArrayBufferAvailable()
  }

  // Metadata read + chunk-shape probe, memoised per (store, array path):
  // only the first call on an array pays the store round-trips, so repeat
  // reads served from a warm chunk cache never touch the store at all.
  // codecMeta.chunk_shape is already the probed (possibly corrected) shape.
  // Given `storeOpts`, so the combined signal governs *this call's wait* on
  // the resolution — the resolution itself is shared and runs signal-free.
  const { codecMeta, encodeChunkKey, fillValue } = await resolveArrayInfo(
    arr,
    storeOpts,
  )

  const Ctr = get_ctr(arr.dtype)
  const bytesPerElement = (Ctr as unknown as { BYTES_PER_ELEMENT: number })
    .BYTES_PER_ELEMENT

  // Checkpoint for stores that ignore the signal: their metadata and probe
  // reads complete instead of rejecting — and a memoised resolution never
  // touches the store at all — and this is the last await before the pool
  // (whose own signal handling covers the rest). Without it, a fully-cached
  // read would return data after its caller already walked away. Watches the
  // combined signal so a store-level abort is caught too.
  if (fetchSignal?.aborted) {
    throw fetchSignal.reason
  }

  // Get stable metaId for the codec metadata (used by worker-rpc meta-init)
  const metaId = getMetaId(codecMeta)

  // Set up the indexer with the actual (possibly corrected) chunk shape
  const chunkShape = codecMeta.chunk_shape
  const indexer = new BasicIndexer({
    selection,
    shape: arr.shape,
    chunk_shape: chunkShape,
  })

  // Allocate output — backed by SharedArrayBuffer when requested
  const size = indexer.shape.reduce((a: number, b: number) => a * b, 1)
  const buffer = createBuffer(size * bytesPerElement, useShared)
  const data = new Ctr(buffer as ArrayBuffer, 0, size)
  const outStride = get_strides(indexer.shape)
  const out = setter.prepare(data, indexer.shape, outStride) as Chunk<D>

  // Build tasks — one per chunk
  const tasks: WorkerPoolTask<void>[] = []

  for (const { chunk_coords, mapping } of indexer) {
    const chunkKey = encodeChunkKey(chunk_coords)
    const chunkPath = arr.resolve(chunkKey).path

    // Compute edge chunk shape: min(chunk_shape[d], array_shape[d] - coord * chunk_shape[d])
    const edgeChunkShape = chunk_coords.map((coord, dim) =>
      Math.min(chunkShape[dim], arr.shape[dim] - coord * chunkShape[dim]),
    )
    const isEdgeChunk = edgeChunkShape.some((s, i) => s !== chunkShape[i])

    // Check cache before building the task — cache hits skip the worker entirely
    const cacheKey = createCacheKey(arr, encodeChunkKey, chunk_coords)
    const cachedChunk = cache.get(cacheKey)

    if (cachedChunk) {
      // Cache hit — copy cached decoded chunk into output on main thread.
      // No worker needed, no fetch, no decompression.
      setter.set_from_chunk(out, cachedChunk as Chunk<D>, mapping)
      continue
    }

    /** The zero/fill-value chunk used when the store has no bytes for this key. */
    const buildFillChunk = (): Chunk<D> => {
      const fillChunkShape = edgeChunkShape
      const fillChunkStrides = get_strides(fillChunkShape)
      const fillChunkSize = fillChunkShape.reduce(
        (a: number, b: number) => a * b,
        1,
      )
      const chunkData = new Ctr(fillChunkSize)
      if (fillValue != null) {
        // @ts-expect-error: fill_value type is union
        chunkData.fill(fillValue)
      }
      return {
        data: chunkData as Chunk<D>["data"],
        shape: fillChunkShape,
        stride: fillChunkStrides,
      }
    }

    /**
     * The task proper. Wrapped below so a worker created *by the task* is
     * terminated when the task throws: the pool terminates the slot it lent
     * on failure, but a worker created here never reached the pool — the
     * `{ worker }` return that would have introduced it is exactly what a
     * throw skips — so nothing else can shut it down, and a leaked
     * `node:worker_threads` thread keeps the whole process alive. An aborted
     * fetch is the common way to land here.
     */
    const runTask = async (
      worker: WorkerLike,
    ): Promise<{ worker: WorkerLike; result: void }> => {
      // SAB path (no cache): the worker decodes AND writes directly into *this*
      // call's SharedArrayBuffer, using *this* call's mapping — no transfer
      // back, no main-thread copy. There is no standalone chunk here to hand to
      // anyone else, so this path cannot participate in sharing and is left
      // exactly as it was.
      if (useShared && !opts.cache) {
        const rawBytes = await arr.store.get(chunkPath, storeOpts)
        if (!rawBytes) {
          setter.set_from_chunk(out, buildFillChunk(), mapping)
        } else {
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
              isEdgeChunk ? edgeChunkShape : undefined,
            )
          } catch (error) {
            worker.terminate()
            throw error
          }
        }
        return { worker, result: undefined as void }
      }

      // The cache is consulted again here, not just when the task list was
      // built: between those two moments another task — very likely a
      // concurrent `getWorker` — may have finished this exact chunk.
      const cachedSinceBuild = cache.get(cacheKey)
      if (cachedSinceBuild) {
        setter.set_from_chunk(out, cachedSinceBuild as Chunk<D>, mapping)
        return { worker, result: undefined as void }
      }

      // One fetch and one decode per chunk, however many callers want it. The
      // follower still holds its worker slot while waiting, which costs some
      // parallelism — but that slot would otherwise have been spent on a
      // duplicate network round-trip and a duplicate decompression of bytes
      // already in flight, so it is not work being given up.
      const produce = async (): Promise<Chunk<D>> => {
        const rawBytes = await arr.store.get(chunkPath, storeOpts)
        if (!rawBytes) {
          return buildFillChunk()
        }
        try {
          return await workerDecode<D>(
            worker,
            rawBytes,
            metaId,
            codecMeta,
            isEdgeChunk ? edgeChunkShape : undefined,
          )
        } catch (error) {
          worker.terminate()
          throw error
        }
      }

      // The shared fetch runs with its *producer's* signal, so a concurrent
      // read aborting can fail a chunk this read still wants. Such a foreign
      // abort — a fired producer signal that is not our own — is retried: the
      // settled entry has already been forgotten, so the retry produces (or
      // joins) a fresh in-flight fetch carrying this read's own signal. Our
      // own abort, and every real failure, still propagates. The loop cannot
      // spin on itself: once we are the producer, `producerSignal` is
      // `fetchSignal` and the foreign-abort test can no longer pass.
      let chunk: Chunk<D>
      for (;;) {
        const { promise, producerSignal } = shareInFlightChunk<D>(
          cacheKey,
          produce,
          fetchSignal,
        )
        try {
          chunk = await promise
          break
        } catch (error) {
          const foreignAbort =
            producerSignal !== fetchSignal && producerSignal?.aborted === true
          if (!foreignAbort || fetchSignal?.aborted) {
            throw error
          }
        }
      }

      // Populate *this* read's cache, whoever produced the chunk. Sharing is
      // keyed on the chunk, not on the cache, so the producer may have been a
      // concurrent read holding a different cache instance — or none at all.
      // Leaving the write to the producer would mean a caller that supplied a
      // cache silently not getting it filled, which is the `cache` contract
      // ("on a cache miss the decoded chunk is stored for future use") quietly
      // not holding.
      //
      // Guarded rather than unconditional so no cache is handed an entry it
      // already holds: with a shared chunk that write is not merely redundant,
      // it displaces a live entry with itself, which a cache that disposes on
      // overwrite would act on.
      if (!cache.get(cacheKey)) {
        cache.set(cacheKey, chunk)
      }

      setter.set_from_chunk(out, chunk, mapping)

      return { worker, result: undefined as void }
    }

    tasks.push(async (workerSlot: WorkerLike | null) => {
      const worker = workerSlot ?? createCodecWorker(workerUrl)
      try {
        return await runTask(worker)
      } catch (error) {
        if (workerSlot == null) {
          worker.terminate()
        }
        throw error
      }
    })
  }

  // Execute all tasks with bounded concurrency via WorkerPool. The combined
  // signal is handed to the pool, which drops still-queued tasks when either
  // source fires and rejects the run with that signal's reason — with only
  // `signal`, a store-level abort would leave queued tasks dispatching just
  // to fail on their own fetches.
  if (tasks.length > 0) {
    const { promise } = pool.runTasks(tasks, null, { signal: fetchSignal })
    await promise
  }

  // If the final shape is empty (all integer selections), return a scalar
  if (indexer.shape.length === 0) {
    const unwrap =
      "get" in out.data
        ? (out.data as unknown as { get(idx: number): Scalar<D> }).get(0)
        : (out.data as unknown as ArrayLike<Scalar<D>>)[0]
    // @ts-expect-error: TS can't narrow conditional type
    return unwrap
  }

  // @ts-expect-error: TS can't narrow conditional type
  return out
}
