/**
 * Types for @fideus-labs/fizarrita
 *
 * Includes worker message protocol types, options for getWorker/setWorker,
 * and codec chunk metadata for reconstructing codec pipelines in workers.
 */

import type { WorkerPool } from '@fideus-labs/worker-pool'
import type { CodecMetadata, DataType, Readable } from 'zarrita'

// ---------------------------------------------------------------------------
// Codec chunk metadata — sent to the worker to reconstruct the codec pipeline
// ---------------------------------------------------------------------------

/**
 * Minimal metadata needed to reconstruct a zarrita codec pipeline in a worker.
 * Matches the shape of zarrita's internal `ChunkMetadata` type.
 */
export interface CodecChunkMeta {
  data_type: DataType
  chunk_shape: number[]
  codecs: CodecMetadata[]
}

// ---------------------------------------------------------------------------
// Worker message protocol
// ---------------------------------------------------------------------------

export interface InitRequest {
  type: 'init'
  id: number
  metaId: number
  meta: CodecChunkMeta
}

export interface InitResponse {
  type: 'init_ok'
  id: number
}

export interface DecodeRequest {
  type: 'decode'
  id: number
  bytes: ArrayBuffer
  metaId: number
  /**
   * Actual shape of this chunk, accounting for edge chunks that may be
   * smaller than chunk_shape. When omitted, chunk_shape from metadata is used.
   */
  actualChunkShape?: number[]
}

export interface DecodeResponse {
  type: 'decoded'
  id: number
  data: ArrayBuffer
  shape: number[]
  stride: number[]
}

export interface EncodeRequest {
  type: 'encode'
  id: number
  data: ArrayBuffer
  metaId: number
}

export interface EncodeResponse {
  type: 'encoded'
  id: number
  bytes: ArrayBuffer
}

// ---------------------------------------------------------------------------
// Projection types — plain serializable data describing chunk→output mapping.
// Mirrors zarrita's Projection type but defined locally for worker messages.
// ---------------------------------------------------------------------------

/** [start, stop, step] index range. */
export type Indices = [start: number, stop: number, step: number]

/** Describes how one dimension of a decoded chunk maps into the output array. */
export type Projection =
  | { from: null; to: number }       // integer index on source side (dim collapse)
  | { from: number; to: null }       // integer index on dest side (dim collapse)
  | { from: Indices; to: Indices }   // slice-to-slice mapping

// ---------------------------------------------------------------------------
// Decode-into-shared protocol — worker decodes and writes directly into SAB
// ---------------------------------------------------------------------------

export interface DecodeIntoRequest {
  type: 'decode_into'
  id: number
  /** Raw encoded chunk bytes (transferred one-way to worker). */
  bytes: ArrayBuffer
  metaId: number
  /** The shared output buffer — NOT transferred, shared via structured clone. */
  output: SharedArrayBuffer
  /** Total byte length of the output typed array. */
  outputByteLength: number
  /** Strides of the output array (in elements, not bytes). */
  outputStride: number[]
  /** Mapping from decoded chunk positions to output positions. */
  projections: Projection[]
  /** sizeof one element in bytes (e.g. 4 for int32/float32). */
  bytesPerElement: number
  /**
   * Actual shape of this chunk, accounting for edge chunks that may be
   * smaller than chunk_shape. When omitted, chunk_shape from metadata is used.
   */
  actualChunkShape?: number[]
}

export interface DecodeIntoResponse {
  type: 'decode_into_ok'
  id: number
}

export type WorkerRequest = InitRequest | DecodeRequest | EncodeRequest | DecodeIntoRequest
export type WorkerResponse = InitResponse | DecodeResponse | EncodeResponse | DecodeIntoResponse

// ---------------------------------------------------------------------------
// Options for getWorker / setWorker
// ---------------------------------------------------------------------------

export interface GetWorkerOptions<StoreOpts = unknown> {
  /** The WorkerPool to use for codec decode operations. */
  pool: WorkerPool
  /** Pass-through options for the store's `get` method (e.g., RequestInit for FetchStore). */
  opts?: StoreOpts
  /**
   * URL of the codec worker script. If not provided, uses the default
   * codec-worker bundled with this package.
   */
  workerUrl?: string | URL
  /**
   * When true, the returned Chunk's TypedArray is backed by SharedArrayBuffer,
   * enabling zero-copy sharing with other Web Workers.
   *
   * When SharedArrayBuffer is available, codec workers also decode directly
   * into the shared output buffer, eliminating one ArrayBuffer transfer and
   * one main-thread copy per chunk.
   *
   * Requires Cross-Origin-Opener-Policy and Cross-Origin-Embedder-Policy
   * headers to be set. Throws if SharedArrayBuffer is not available.
   */
  useSharedArrayBuffer?: boolean
}

export interface SetWorkerOptions {
  /** The WorkerPool to use for codec encode/decode operations. */
  pool: WorkerPool
  /**
   * URL of the codec worker script. If not provided, uses the default
   * codec-worker bundled with this package.
   */
  workerUrl?: string | URL
  /**
   * When true, intermediate chunk buffers for partial updates are allocated
   * on SharedArrayBuffer, enabling zero-transfer sharing between the main
   * thread and codec workers during the decode-modify-encode cycle.
   *
   * Requires Cross-Origin-Opener-Policy and Cross-Origin-Embedder-Policy
   * headers to be set. Throws if SharedArrayBuffer is not available.
   */
  useSharedArrayBuffer?: boolean
}
