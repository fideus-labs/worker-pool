/**
 * @fideus-labs/fizarrita — Worker-pool-accelerated get/set for zarrita.
 *
 * Provides `getWorker` and `setWorker` as drop-in replacements for zarrita's
 * `get` and `set` that offload codec encode/decode to Web Workers via a
 * WorkerPool with bounded concurrency.
 */

export { getWorker } from './get-worker.js'
export { setWorker } from './set-worker.js'
export type {
  GetWorkerOptions,
  SetWorkerOptions,
  CodecChunkMeta,
} from './types.js'
