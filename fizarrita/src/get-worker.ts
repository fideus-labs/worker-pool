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
import { create_codec_pipeline } from './internals/codec-pipeline.js'
import { setter } from './internals/setter.js'
import {
  get_ctr,
  get_strides,
  create_chunk_key_encoder,
  assertSharedArrayBufferAvailable,
  createBuffer,
} from './internals/util.js'
import type { GetWorkerOptions, CodecChunkMeta, ChunkCache } from './types.js'
import { workerDecode, workerDecodeInto, getMetaId } from './worker-rpc.js'

/**
 * Default URL for the codec worker. Uses `import.meta.url` to resolve
 * relative to this module.
 */
export const DEFAULT_WORKER_URL = new URL('./codec-worker.js', import.meta.url)

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

// ---------------------------------------------------------------------------
// Unified metadata reader — reads zarr.json once, returns everything needed
// ---------------------------------------------------------------------------

export interface ArrayMetadata {
  codecMeta: CodecChunkMeta
  encodeChunkKey: (chunk_coords: number[]) => string
  fillValue: Scalar<DataType> | null
}

export async function readArrayMetadata<D extends DataType, Store extends Readable>(
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
export function readZstdFrameContentSize(compressed: Uint8Array): number | null {
  if (compressed.length < 6) return null

  const magic =
    compressed[0] |
    (compressed[1] << 8) |
    (compressed[2] << 16) |
    (compressed[3] << 24)
  if ((magic >>> 0) !== 0xfd2fb528) return null

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
export function readBloscFrameContentSize(compressed: Uint8Array): number | null {
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
 * Try to determine the decompressed byte size of a raw chunk without full decoding.
 *
 * Hybrid strategy (cheapest first):
 *  1. Zstd frame header — read FCS field (zero-cost, no decompression)
 *  2. Blosc header — read nbytes field (zero-cost, no decompression)
 *  3. Uncompressed check — if raw byte count matches a plausible element count,
 *     the chunk may be uncompressed (bytes codec only)
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

  // 3. Check if the raw bytes could be an uncompressed chunk.
  //    For the bytes codec (no compression), rawBytes.byteLength IS the
  //    decompressed size. We check whether the codec chain is bytes-only
  //    (no bytes_to_bytes compression codecs).
  const hasCompression = codecMeta.codecs.some((c) => {
    const name = c.name.toLowerCase()
    // array_to_array codecs (transpose, etc.) don't change byte size
    // array_to_bytes codecs (bytes, etc.) don't compress
    // bytes_to_bytes codecs are the compressors
    return (
      name === 'gzip' ||
      name === 'zlib' ||
      name === 'blosc' ||
      name === 'zstd' ||
      name === 'lz4' ||
      name === 'bz2' ||
      name === 'lzma' ||
      name === 'snappy'
    )
  })
  if (!hasCompression) {
    // No compression codec — raw bytes are the decompressed data
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
    for (let i = 0; i < shape.length; i++) l1 += Math.abs(shape[i] - metadataChunkShape[i])

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
    const key = shape.join(',')
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
    if (candidate > 0 && candidate <= arrayShape[vary] && Number.isInteger(candidate)) {
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
 * Returns true if the candidate is valid (probe returned 404/empty),
 * false if invalid (probe returned data, meaning chunks are too coarse).
 */
async function validateCandidateChunkShape<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
  encodeChunkKey: (chunk_coords: number[]) => string,
  candidate: number[],
  storeOpts?: Parameters<Store['get']>[1],
): Promise<boolean> {
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
    // All dimensions have only 1 chunk — can't validate, assume correct
    return true
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
    return !probeBytes
  } catch {
    // Fetch error (404, network error) → no chunk there → accept
    return true
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
export async function probeActualChunkShape<D extends DataType, Store extends Readable>(
  arr: ZarrArray<D, Store>,
  encodeChunkKey: (chunk_coords: number[]) => string,
  codecMeta: CodecChunkMeta,
  bytesPerElement: number,
  storeOpts?: Parameters<Store['get']>[1],
): Promise<number[]> {
  const metadataChunkShape = codecMeta.chunk_shape
  const metaElements = metadataChunkShape.reduce((a, b) => a * b, 1)

  // Fetch the first chunk (c/0/0/0)
  const zeroCoords = metadataChunkShape.map(() => 0)
  const chunkKey = encodeChunkKey(zeroCoords)
  const chunkPath = arr.resolve(chunkKey).path

  try {
    const rawBytes = await arr.store.get(chunkPath, storeOpts)
    if (!rawBytes) return metadataChunkShape

    // Determine decompressed size via hybrid strategy
    const decompressedBytes = await probeDecompressedSize(rawBytes, codecMeta, bytesPerElement)
    if (decompressedBytes == null) return metadataChunkShape

    const actualElements = decompressedBytes / bytesPerElement
    if (actualElements === metaElements) return metadataChunkShape

    // Mismatch detected — infer chunk shape from element count + heuristics
    const candidates = inferChunkShape(actualElements, metadataChunkShape, arr.shape)
    if (candidates.length === 0) return metadataChunkShape

    // Validate candidates by probing one-past-the-end.
    // The first candidate that passes validation wins.
    // Limit validation attempts to avoid excessive network requests.
    const maxValidationAttempts = Math.min(candidates.length, 5)
    for (let i = 0; i < maxValidationAttempts; i++) {
      const candidate = candidates[i]
      const isValid = await validateCandidateChunkShape(arr, encodeChunkKey, candidate, storeOpts)
      if (isValid) {
        console.warn(
          `[fizarrita] Metadata chunk_shape ${JSON.stringify(metadataChunkShape)} ` +
            `does not match actual chunk data (${actualElements} elements). ` +
            `Using inferred chunk_shape: ${JSON.stringify(candidate)}`,
        )
        return candidate
      }
    }

    // No candidate passed validation — fall back to best-scored
    const fallback = candidates[0]
    console.warn(
      `[fizarrita] Metadata chunk_shape ${JSON.stringify(metadataChunkShape)} ` +
        `does not match actual chunk data (${actualElements} elements). ` +
        `Using inferred chunk_shape: ${JSON.stringify(fallback)} (unvalidated)`,
    )
    return fallback
  } catch {
    return metadataChunkShape
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
 * import { getWorker } from '@fideus-labs/fizarrita'
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
  const cache = opts.cache ?? NULL_CACHE

  if (useShared) {
    assertSharedArrayBufferAvailable()
  }

  // Read metadata from store — single read, single parse
  const { codecMeta, encodeChunkKey, fillValue } = await readArrayMetadata(arr)

  const Ctr = get_ctr(arr.dtype)
  const bytesPerElement = (Ctr as unknown as { BYTES_PER_ELEMENT: number }).BYTES_PER_ELEMENT

  // Probe actual chunk shape — detects metadata vs data mismatch
  const actualChunkShape = await probeActualChunkShape(arr, encodeChunkKey, codecMeta, bytesPerElement, opts.opts)

  // Update codecMeta to use the actual chunk shape for codec pipeline
  const correctedCodecMeta = actualChunkShape !== codecMeta.chunk_shape
    ? { ...codecMeta, chunk_shape: actualChunkShape }
    : codecMeta

  // Get stable metaId for the codec metadata (used by worker-rpc meta-init)
  const metaId = getMetaId(correctedCodecMeta)

  // Set up the indexer with the actual (possibly corrected) chunk shape
  const indexer = new BasicIndexer({
    selection,
    shape: arr.shape,
    chunk_shape: actualChunkShape,
  })

  // Allocate output — backed by SharedArrayBuffer when requested
  const size = indexer.shape.reduce((a: number, b: number) => a * b, 1)
  const buffer = createBuffer(size * bytesPerElement, useShared)
  const data = new Ctr(buffer as ArrayBuffer, 0, size)
  const outStride = get_strides(indexer.shape)
  const out = setter.prepare(data, indexer.shape, outStride) as Chunk<D>

  // Pre-compute chunk invariants (hoisted out of loop)
  const chunkShape = actualChunkShape

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

    tasks.push(async (workerSlot: Worker | null) => {
      const worker = workerSlot ?? new Worker(resolvedWorkerUrl, { type: 'module' })

      // Fetch raw bytes from store on main thread
      const rawBytes = await arr.store.get(chunkPath, opts.opts)

      if (!rawBytes) {
        // Missing chunk — fill value, no worker needed
        const fillChunkShape = edgeChunkShape
        const fillChunkStrides = get_strides(fillChunkShape)
        const fillChunkSize = fillChunkShape.reduce((a: number, b: number) => a * b, 1)
        const chunkData = new Ctr(fillChunkSize)
        if (fillValue != null) {
          // @ts-expect-error: fill_value type is union
          chunkData.fill(fillValue)
        }
        const chunk: Chunk<D> = {
          data: chunkData as Chunk<D>['data'],
          shape: fillChunkShape,
          stride: fillChunkStrides,
        }
        // Cache the fill-value chunk
        cache.set(cacheKey, chunk)
        // Copy fill-value chunk into output on main thread
        setter.set_from_chunk(out, chunk, mapping)
      } else if (useShared && !opts.cache) {
        // SAB path (no cache): worker decodes AND writes directly into the
        // SharedArrayBuffer output — no transfer back, no main-thread copy.
        try {
          await workerDecodeInto(
            worker,
            rawBytes,
            metaId,
            correctedCodecMeta,
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
      } else if (useShared && opts.cache) {
        // SAB path with cache: use workerDecode to get a standalone chunk
        // so we can cache it, then copy into the SAB output. The small
        // overhead of transfer + copy on first access is repaid by
        // subsequent cache hits that skip the worker entirely.
        let chunk: Chunk<D>
        try {
          chunk = await workerDecode<D>(worker, rawBytes, metaId, correctedCodecMeta, isEdgeChunk ? edgeChunkShape : undefined)
        } catch (error) {
          worker.terminate()
          throw error
        }
        cache.set(cacheKey, chunk)
        setter.set_from_chunk(out, chunk, mapping)
      } else {
        // Standard path: worker decodes, transfers back, main thread copies
        let chunk: Chunk<D>
        try {
          chunk = await workerDecode<D>(worker, rawBytes, metaId, correctedCodecMeta, isEdgeChunk ? edgeChunkShape : undefined)
        } catch (error) {
          worker.terminate()
          throw error
        }
        cache.set(cacheKey, chunk)
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
