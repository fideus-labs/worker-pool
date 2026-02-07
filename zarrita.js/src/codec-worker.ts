/**
 * Web Worker that handles encode/decode operations using zarrita's codec pipeline.
 *
 * Uses our self-contained codec pipeline builder (which references zarrita's
 * publicly exported `registry`) to decode/encode chunks with real codecs
 * (gzip, blosc, zstd, bytes, transpose, etc.).
 *
 * Message protocol:
 *   decode: { type: 'decode', id, bytes: ArrayBuffer, meta: CodecChunkMeta }
 *           -> { type: 'decoded', id, data: ArrayBuffer, shape, stride }
 *
 *   encode: { type: 'encode', id, data: ArrayBuffer, meta: CodecChunkMeta }
 *           -> { type: 'encoded', id, bytes: ArrayBuffer }
 */

import { create_codec_pipeline } from './internals/codec-pipeline.js'
import { get_ctr, get_strides } from './internals/util.js'
import type { Chunk, DataType } from 'zarrita'
import type { CodecChunkMeta, WorkerRequest } from './types.js'

const ctx = self as unknown as DedicatedWorkerGlobalScope

// ---------------------------------------------------------------------------
// Codec pipeline cache — avoid recreating pipelines for the same array config
// ---------------------------------------------------------------------------

const pipelineCache = new Map<
  string,
  ReturnType<typeof create_codec_pipeline>
>()

function getOrCreatePipeline(meta: CodecChunkMeta) {
  const key = JSON.stringify(meta)
  let pipeline = pipelineCache.get(key)
  if (!pipeline) {
    pipeline = create_codec_pipeline({
      data_type: meta.data_type,
      shape: meta.chunk_shape,
      codecs: meta.codecs,
    })
    pipelineCache.set(key, pipeline)
  }
  return pipeline
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------

ctx.addEventListener('message', async (event: MessageEvent<WorkerRequest>) => {
  const msg = event.data

  try {
    if (msg.type === 'decode') {
      const pipeline = getOrCreatePipeline(msg.meta)
      const bytes = new Uint8Array(msg.bytes)
      const chunk = await pipeline.decode(bytes) as Chunk<DataType>

      // Extract the underlying ArrayBuffer from the decoded TypedArray
      const dataView = chunk.data as unknown as {
        buffer: ArrayBuffer
        byteOffset: number
        byteLength: number
      }
      const buffer = dataView.buffer
      const byteOffset = dataView.byteOffset
      const byteLength = dataView.byteLength

      // If the decoded data doesn't own the full buffer, copy for clean transfer
      let transferBuffer: ArrayBuffer
      if (byteOffset === 0 && byteLength === buffer.byteLength) {
        transferBuffer = buffer
      } else {
        transferBuffer = buffer.slice(byteOffset, byteOffset + byteLength)
      }

      ctx.postMessage(
        {
          type: 'decoded' as const,
          id: msg.id,
          data: transferBuffer,
          shape: chunk.shape,
          stride: chunk.stride,
        },
        [transferBuffer],
      )
    } else if (msg.type === 'encode') {
      const pipeline = getOrCreatePipeline(msg.meta)

      // Reconstruct a Chunk from the transferred ArrayBuffer
      const Ctr = get_ctr(msg.meta.data_type) as unknown as {
        new (buf: ArrayBuffer, off: number, len: number): unknown
        BYTES_PER_ELEMENT: number
      }
      const data = new Ctr(
        msg.data,
        0,
        msg.data.byteLength / Ctr.BYTES_PER_ELEMENT,
      )
      const shape = msg.meta.chunk_shape
      const stride = get_strides(shape, 'C')

      const chunk = { data, shape, stride } as Chunk<DataType>
      const encoded = await pipeline.encode(chunk)

      // Transfer the encoded bytes back
      const transferBuffer =
        encoded.byteOffset === 0 && encoded.byteLength === encoded.buffer.byteLength
          ? (encoded.buffer as ArrayBuffer)
          : encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength)

      ctx.postMessage(
        {
          type: 'encoded' as const,
          id: msg.id,
          bytes: transferBuffer,
        },
        [transferBuffer],
      )
    }
  } catch (error) {
    // Send error back to main thread
    ctx.postMessage({
      type: msg.type === 'decode' ? 'decoded' : 'encoded',
      id: msg.id,
      error: error instanceof Error ? error.message : String(error),
    })
  }
})
