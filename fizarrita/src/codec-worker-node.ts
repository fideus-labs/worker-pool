/**
 * `node:worker_threads` entry point for the codec worker.
 *
 * The Node counterpart of `codec-worker.ts`: same protocol, same pipeline cache
 * (see `internals/codec-worker-core.ts`), different plumbing — `parentPort`
 * instead of a `DedicatedWorkerGlobalScope`.
 *
 * `node:worker_threads` is imported through a variable specifier so bundlers
 * building for the browser never try to resolve it. Awaiting that import at the
 * top level is safe: a `MessagePort` queues incoming messages until the first
 * `'message'` listener is attached, so nothing posted before this module
 * finishes evaluating is lost.
 */

import {
  type CodecWorkerMessage,
  handleCodecMessage,
} from "./internals/codec-worker-core.js"

interface ParentPort {
  on(event: "message", listener: (value: CodecWorkerMessage) => void): void
  postMessage(value: unknown, transferList?: unknown[]): void
}

const specifier = "node:worker_threads"
const { parentPort } = (await import(
  /* @vite-ignore */ /* webpackIgnore: true */ specifier
)) as {
  parentPort: ParentPort | null
}

if (parentPort === null) {
  throw new Error(
    "codec-worker-node.js must be run as a worker thread, not imported on the main thread.",
  )
}

const port = parentPort

port.on("message", async (msg: CodecWorkerMessage) => {
  const reply = await handleCodecMessage(msg)
  if (!reply) return
  try {
    port.postMessage(reply.response, reply.transfer)
  } catch (error) {
    // Posting can still fail on its own — an unclonable value, a buffer that is
    // no longer transferable. Retry without the payload so the caller gets a
    // rejection instead of a request that never settles.
    port.postMessage({
      type: reply.response.type,
      id: reply.response.id,
      error: error instanceof Error ? error.message : String(error),
    })
  }
})
