/**
 * Web Worker that handles encode/decode operations using zarrita's codec pipeline.
 *
 * Uses our self-contained codec pipeline builder (which references zarrita's
 * publicly exported `registry`) to decode/encode chunks with real codecs
 * (gzip, blosc, zstd, bytes, transpose, etc.).
 *
 * Message protocol:
 *   init:   { type: 'init', id, metaId, meta: CodecChunkMeta }
 *           -> { type: 'init_ok', id }
 *
 *   decode: { type: 'decode', id, bytes: ArrayBuffer, metaId }
 *           -> { type: 'decoded', id, data: ArrayBuffer, shape, stride }
 *
 *   encode: { type: 'encode', id, data: ArrayBuffer, metaId }
 *           -> { type: 'encoded', id, bytes: ArrayBuffer }
 */

import { create_codec_pipeline } from './internals/codec-pipeline.js'
import { get_ctr, get_strides } from './internals/util.js'
import type { Chunk, DataType } from 'zarrita'
import type { CodecChunkMeta } from './types.js'

const ctx = self as unknown as DedicatedWorkerGlobalScope

// ---------------------------------------------------------------------------
// Codec pipeline cache — keyed by metaId (integer lookup, no JSON.stringify)
// ---------------------------------------------------------------------------

const pipelineByMetaId = new Map<
  number,
  ReturnType<typeof create_codec_pipeline>
>()

const metaByMetaId = new Map<number, CodecChunkMeta>()

// Legacy fallback: pipeline cache keyed by JSON string (for any decode/encode
// messages that arrive with a full `meta` object instead of `metaId`)
const pipelineByKey = new Map<
  string,
  ReturnType<typeof create_codec_pipeline>
>()

function getPipeline(metaId: number): ReturnType<typeof create_codec_pipeline> {
  const pipeline = pipelineByMetaId.get(metaId)
  if (!pipeline) {
    throw new Error(`No pipeline for metaId ${metaId}. Send an 'init' message first.`)
  }
  return pipeline
}

function getOrCreatePipelineLegacy(meta: CodecChunkMeta) {
  const key = JSON.stringify(meta)
  let pipeline = pipelineByKey.get(key)
  if (!pipeline) {
    pipeline = create_codec_pipeline({
      data_type: meta.data_type,
      shape: meta.chunk_shape,
      codecs: meta.codecs,
    })
    pipelineByKey.set(key, pipeline)
  }
  return pipeline
}

// ---------------------------------------------------------------------------
// Message handler
// ---------------------------------------------------------------------------

type WorkerMessage =
  | { type: 'init'; id: number; metaId: number; meta: CodecChunkMeta }
  | { type: 'decode'; id: number; bytes: ArrayBuffer; metaId?: number; meta?: CodecChunkMeta }
  | { type: 'encode'; id: number; data: ArrayBuffer; metaId?: number; meta?: CodecChunkMeta }

ctx.addEventListener('message', async (event: MessageEvent<WorkerMessage>) => {
  const msg = event.data

  try {
    if (msg.type === 'init') {
      // Register codec metadata and pre-create pipeline
      metaByMetaId.set(msg.metaId, msg.meta)
      const pipeline = create_codec_pipeline({
        data_type: msg.meta.data_type,
        shape: msg.meta.chunk_shape,
        codecs: msg.meta.codecs,
      })
      pipelineByMetaId.set(msg.metaId, pipeline)
      ctx.postMessage({ type: 'init_ok', id: msg.id })
      return
    }

    if (msg.type === 'decode') {
      // Resolve pipeline: prefer metaId, fall back to legacy meta
      const pipeline = msg.metaId !== undefined && pipelineByMetaId.has(msg.metaId)
        ? getPipeline(msg.metaId)
        : getOrCreatePipelineLegacy(msg.meta!)

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
      // Resolve pipeline and meta
      let pipeline: ReturnType<typeof create_codec_pipeline>
      let meta: CodecChunkMeta
      if (msg.metaId !== undefined && pipelineByMetaId.has(msg.metaId)) {
        pipeline = getPipeline(msg.metaId)
        meta = metaByMetaId.get(msg.metaId)!
      } else {
        meta = msg.meta!
        pipeline = getOrCreatePipelineLegacy(meta)
      }

      // Reconstruct a Chunk from the transferred ArrayBuffer
      const Ctr = get_ctr(meta.data_type) as unknown as {
        new (buf: ArrayBuffer, off: number, len: number): unknown
        BYTES_PER_ELEMENT: number
      }
      const data = new Ctr(
        msg.data,
        0,
        msg.data.byteLength / Ctr.BYTES_PER_ELEMENT,
      )
      const shape = meta.chunk_shape
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
      type: msg.type === 'decode' ? 'decoded' : msg.type === 'encode' ? 'encoded' : 'init_ok',
      id: msg.id,
      error: error instanceof Error ? error.message : String(error),
    })
  }
})
