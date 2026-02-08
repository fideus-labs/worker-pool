/**
 * Web Worker that handles encode/decode operations using zarrita's codec pipeline.
 *
 * Uses our self-contained codec pipeline builder (which references zarrita's
 * publicly exported `registry`) to decode/encode chunks with real codecs
 * (gzip, blosc, zstd, bytes, transpose, etc.).
 *
 * Message protocol:
 *   init:        { type: 'init', id, metaId, meta: CodecChunkMeta }
 *                -> { type: 'init_ok', id }
 *
 *   decode:      { type: 'decode', id, bytes: ArrayBuffer, metaId }
 *                -> { type: 'decoded', id, data: ArrayBuffer, shape, stride }
 *
 *   decode_into: { type: 'decode_into', id, bytes, metaId, output: SAB, ... }
 *                -> { type: 'decode_into_ok', id }
 *                Worker decodes and writes directly into SharedArrayBuffer.
 *
 *   encode:      { type: 'encode', id, data: ArrayBuffer, metaId }
 *                -> { type: 'encoded', id, bytes: ArrayBuffer }
 */

import { create_codec_pipeline } from './internals/codec-pipeline.js'
import { compat_chunk, set_from_chunk_binary } from './internals/setter.js'
import { get_ctr, get_strides } from './internals/util.js'
import type { Chunk, DataType } from 'zarrita'
import type { CodecChunkMeta, Projection } from './types.js'

const ctx = self as unknown as DedicatedWorkerGlobalScope

// ---------------------------------------------------------------------------
// Edge chunk shape correction
// ---------------------------------------------------------------------------

/**
 * Fix a decoded chunk's shape and stride when the actual data size differs
 * from the metadata chunk_shape. This happens with edge chunks — zarr v3
 * stores edge chunks at their actual smaller size, but our codec pipeline
 * always reports shape = chunk_shape from metadata.
 *
 * If actualChunkShape is provided, use it directly. Otherwise, infer from
 * the decoded data size vs the metadata chunk_shape.
 */
function fixEdgeChunkShapeStride<D extends DataType>(
  chunk: Chunk<D>,
  actualChunkShape?: number[],
): Chunk<D> {
  if (actualChunkShape) {
    // Caller told us the actual shape — use it directly
    const expectedElements = actualChunkShape.reduce((a, b) => a * b, 1)
    const actualElements = (chunk.data as unknown as ArrayLike<unknown>).length
    if (actualElements === expectedElements) {
      return {
        data: chunk.data,
        shape: actualChunkShape,
        stride: get_strides(actualChunkShape, 'C'),
      }
    }
  }
  // No correction needed or no actualChunkShape provided
  return chunk
}

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
  | { type: 'decode'; id: number; bytes: ArrayBuffer; metaId?: number; meta?: CodecChunkMeta; actualChunkShape?: number[] }
  | { type: 'decode_into'; id: number; bytes: ArrayBuffer; metaId: number; output: SharedArrayBuffer; outputByteLength: number; outputStride: number[]; projections: Projection[]; bytesPerElement: number; actualChunkShape?: number[] }
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
      let chunk = await pipeline.decode(bytes) as Chunk<DataType>

      // Fix shape/stride for edge chunks (smaller than metadata chunk_shape)
      chunk = fixEdgeChunkShapeStride(chunk, msg.actualChunkShape)

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
    } else if (msg.type === 'decode_into') {
      // Decode and write directly into SharedArrayBuffer — no transfer back
      const pipeline = getPipeline(msg.metaId)
      const bytes = new Uint8Array(msg.bytes)
      let chunk = await pipeline.decode(bytes) as Chunk<DataType>

      // Fix shape/stride for edge chunks (smaller than metadata chunk_shape)
      chunk = fixEdgeChunkShapeStride(chunk, msg.actualChunkShape)

      // Create a Uint8Array view over the shared output buffer
      const destView = new Uint8Array(msg.output, 0, msg.outputByteLength)

      // Convert decoded chunk to byte-level representation
      const src = compat_chunk(chunk)

      // Write decoded data directly into the shared output memory
      set_from_chunk_binary(
        { data: destView, stride: msg.outputStride },
        src,
        msg.bytesPerElement,
        msg.projections,
      )

      ctx.postMessage({ type: 'decode_into_ok', id: msg.id })
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
      type: msg.type === 'decode' ? 'decoded' : msg.type === 'encode' ? 'encoded' : msg.type === 'decode_into' ? 'decode_into_ok' : 'init_ok',
      id: msg.id,
      error: error instanceof Error ? error.message : String(error),
    })
  }
})
