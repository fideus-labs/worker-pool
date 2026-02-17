/**
 * @fideus-labs/fizarrita — Worker-pool-accelerated get/set for zarrita.
 *
 * Provides `getWorker` and `setWorker` as drop-in replacements for zarrita's
 * `get` and `set` that offload codec encode/decode to Web Workers via a
 * WorkerPool with bounded concurrency.
 */

export type { ArrayMetadata } from "./get-worker.js"
export {
  createCacheKey,
  createDefaultWorker,
  /** @deprecated Use {@link createDefaultWorker} instead. */
  DEFAULT_WORKER_URL,
  getStoreId,
  getWorker,
  inferChunkShape,
  probeActualChunkShape,
  readArrayMetadata,
  readBloscFrameContentSize,
  readZstdFrameContentSize,
} from "./get-worker.js"
// Internals — exported for building custom workers that extend the codec worker
export { create_codec_pipeline } from "./internals/codec-pipeline.js"
export type { ChunkProjection, IndexerProjection } from "./internals/indexer.js"
export {
  BasicIndexer,
  normalize_selection,
  slice,
  slice_indices,
} from "./internals/indexer.js"
export {
  compat_chunk,
  set_from_chunk_binary,
  setter,
} from "./internals/setter.js"
export type { ChunkKeyEncoding } from "./internals/util.js"
export {
  assertSharedArrayBufferAvailable,
  create_chunk_key_encoder,
  createBuffer,
  get_ctr,
  get_strides,
} from "./internals/util.js"
export { setWorker } from "./set-worker.js"
export type {
  ChunkCache,
  CodecChunkMeta,
  GetWorkerOptions,
  Indices,
  Projection,
  SetWorkerOptions,
} from "./types.js"
// Worker RPC helpers — for composing custom workers
export {
  getMetaId,
  workerDecode,
  workerDecodeInto,
  workerEncode,
} from "./worker-rpc.js"
