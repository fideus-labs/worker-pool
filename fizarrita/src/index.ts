/**
 * @fideus-labs/fizarrita — Worker-pool-accelerated get/set for zarrita.
 *
 * Provides `getWorker` and `setWorker` as drop-in replacements for zarrita's
 * `get` and `set` that offload codec encode/decode to Web Workers via a
 * WorkerPool with bounded concurrency.
 */

export {
  getWorker,
  readZstdFrameContentSize,
  readBloscFrameContentSize,
  inferChunkShape,
  DEFAULT_WORKER_URL,
  readArrayMetadata,
  probeActualChunkShape,
  createCacheKey,
  getStoreId,
} from './get-worker.js'
export type { ArrayMetadata } from './get-worker.js'
export { setWorker } from './set-worker.js'
export type {
  GetWorkerOptions,
  SetWorkerOptions,
  CodecChunkMeta,
  ChunkCache,
  Projection,
  Indices,
} from './types.js'

// Internals — exported for building custom workers that extend the codec worker
export { create_codec_pipeline } from './internals/codec-pipeline.js'
export {
  get_ctr,
  get_strides,
  create_chunk_key_encoder,
  createBuffer,
  assertSharedArrayBufferAvailable,
} from './internals/util.js'
export type { ChunkKeyEncoding } from './internals/util.js'
export { setter, compat_chunk, set_from_chunk_binary } from './internals/setter.js'
export {
  BasicIndexer,
  normalize_selection,
  slice,
  slice_indices,
} from './internals/indexer.js'
export type { ChunkProjection, IndexerProjection } from './internals/indexer.js'

// Worker RPC helpers — for composing custom workers
export { workerDecode, workerDecodeInto, workerEncode, getMetaId } from './worker-rpc.js'
