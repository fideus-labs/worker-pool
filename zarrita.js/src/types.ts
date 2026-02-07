/**
 * Types for @fideus-labs/zarrita.js
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

export interface DecodeRequest {
  type: 'decode'
  id: number
  bytes: ArrayBuffer
  meta: CodecChunkMeta
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
  meta: CodecChunkMeta
}

export interface EncodeResponse {
  type: 'encoded'
  id: number
  bytes: ArrayBuffer
}

export type WorkerRequest = DecodeRequest | EncodeRequest
export type WorkerResponse = DecodeResponse | EncodeResponse

// ---------------------------------------------------------------------------
// Options for getWorker / setWorker
// ---------------------------------------------------------------------------

export interface GetWorkerOptions<StoreOpts = unknown> {
  /** The WorkerPool to use for codec decode operations. */
  pool: WorkerPool
  /**
   * Maximum number of concurrent chunk operations.
   * Defaults to the pool size (workerQueue initial length).
   */
  concurrency?: number
  /** Pass-through options for the store's `get` method (e.g., RequestInit for FetchStore). */
  opts?: StoreOpts
  /**
   * URL of the codec worker script. If not provided, uses the default
   * codec-worker bundled with this package.
   */
  workerUrl?: string | URL
}

export interface SetWorkerOptions {
  /** The WorkerPool to use for codec encode/decode operations. */
  pool: WorkerPool
  /**
   * Maximum number of concurrent chunk operations.
   * Defaults to the pool size (workerQueue initial length).
   */
  concurrency?: number
  /**
   * URL of the codec worker script. If not provided, uses the default
   * codec-worker bundled with this package.
   */
  workerUrl?: string | URL
}
