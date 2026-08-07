/**
 * Web Worker that handles encode/decode operations using zarrita's codec
 * pipeline.
 *
 * This is the browser entry point: it wires the dedicated worker's message
 * events to {@link handleCodecMessage}, which holds the protocol and the codec
 * pipeline cache. `codec-worker-node.ts` is the `node:worker_threads`
 * equivalent — see that file, and `internals/codec-worker-core.ts` for the
 * message protocol.
 */

import {
  type CodecWorkerMessage,
  handleCodecMessage,
} from "./internals/codec-worker-core.js"

const ctx = self as unknown as DedicatedWorkerGlobalScope

ctx.addEventListener(
  "message",
  async (event: MessageEvent<CodecWorkerMessage>) => {
    const reply = await handleCodecMessage(event.data)
    if (!reply) return
    try {
      ctx.postMessage(reply.response, reply.transfer)
    } catch (error) {
      // Posting can still fail on its own — an unclonable value, a buffer that
      // is no longer transferable. Retry without the payload so the caller gets
      // a rejection instead of a request that never settles.
      ctx.postMessage({
        type: reply.response.type,
        id: reply.response.id,
        error: error instanceof Error ? error.message : String(error),
      })
    }
  },
)
