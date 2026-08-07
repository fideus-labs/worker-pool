/**
 * Codec worker construction, per runtime.
 *
 * Browsers get a module `Worker` running `codec-worker.js`; plain Node, which
 * has no global `Worker` at all, gets a `node:worker_threads` worker running
 * `codec-worker-node.js` behind the same interface. Both entries speak the
 * identical protocol, so everything downstream — `worker-rpc`, `getWorker`,
 * `setWorker`, the pool — is unaware of which runtime it is on.
 */

import {
  createWorker,
  isNodeRuntime,
  NodeWorker,
  type WorkerLike,
} from "@fideus-labs/worker-pool"

/**
 * Default URL for the codec worker. Uses `import.meta.url` to resolve
 * relative to this module.
 *
 * @deprecated Use {@link createDefaultWorker} instead — it produces a
 *   `new Worker(new URL(..., import.meta.url))` expression that bundlers
 *   like Vite recognise as a worker entry point and bundle accordingly, and it
 *   picks the right worker implementation for the runtime.
 */
export const DEFAULT_WORKER_URL = new URL("./codec-worker.js", import.meta.url)

/**
 * Create a worker running the default codec worker bundled with this package.
 *
 * In the browser, `new Worker(new URL(..., import.meta.url))` is written as a
 * single expression so bundlers (Vite, Rollup, webpack 5) detect the worker
 * entry point and bundle its dependency graph into a self-contained asset.
 * Storing the URL in a variable first — as the deprecated
 * {@link DEFAULT_WORKER_URL} does — makes bundlers treat the worker file as a
 * plain static asset, leaving its relative `./internals/*` imports unresolved.
 *
 * The Node branch deliberately does the opposite, keeping its entry behind a
 * variable so bundlers targeting the browser leave it alone.
 */
export function createDefaultWorker(): WorkerLike {
  if (typeof Worker !== "undefined") {
    return new Worker(new URL("./codec-worker.js", import.meta.url), {
      type: "module",
    })
  }
  if (isNodeRuntime()) {
    const nodeEntry = "./codec-worker-node.js"
    return new NodeWorker(new URL(nodeEntry, import.meta.url))
  }
  throw new Error(
    "No Worker implementation available: this runtime has no global `Worker` " +
      "and is not Node.",
  )
}

/**
 * Create the codec worker for a `getWorker`/`setWorker` call: the caller's
 * `workerUrl` when given, otherwise the bundled default.
 */
export function createCodecWorker(workerUrl?: string | URL): WorkerLike {
  return workerUrl === undefined ? createDefaultWorker() : createWorker(workerUrl)
}
