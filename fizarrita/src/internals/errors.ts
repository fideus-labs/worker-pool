/**
 * Error helpers shared by the main-thread entry points and the internals.
 */

import { CodecPipelineError, isZarritaError } from 'zarrita'

/**
 * A codec failure pinned to the store key it happened on. The worker knows
 * the codec but not which chunk it was decoding; the caller knows the chunk.
 * The result is what `zarr.get` would throw, plus `chunkPath` — a shard's
 * key for a sharded array's inner chunk or index — and any other error comes
 * back as it went in.
 */
export function atChunk(error: unknown, chunkPath: string): unknown {
  if (isZarritaError(error, 'CodecPipelineError') && !error.chunkPath) {
    return new CodecPipelineError({
      direction: error.direction,
      codec: error.codec,
      chunkPath,
      cause: error.cause,
    })
  }
  return error
}
