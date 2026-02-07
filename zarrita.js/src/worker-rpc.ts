/**
 * Main-thread helpers for communicating with the codec worker via postMessage.
 *
 * Provides workerDecode() and workerEncode() — each sends a message to the
 * worker, waits for the matching response, and returns the decoded/encoded
 * result. Uses transferable ArrayBuffers for zero-copy data passing.
 */

import type { Chunk, DataType, TypedArray } from 'zarrita'
import { get_ctr } from './internals/util.js'
import type { CodecChunkMeta } from './types.js'

/** Monotonically increasing request ID for matching responses. */
let nextRequestId = 0

/**
 * Send raw bytes to a codec worker for decoding and return the decoded Chunk.
 *
 * @param worker - The worker to send the message to.
 * @param bytes  - Raw chunk bytes (will be copied and transferred, not shared).
 * @param meta   - Codec metadata for reconstructing the pipeline in the worker.
 * @returns Decoded chunk with data, shape, and stride.
 */
export function workerDecode<D extends DataType>(
  worker: Worker,
  bytes: Uint8Array,
  meta: CodecChunkMeta,
): Promise<Chunk<D>> {
  return new Promise((resolve, reject) => {
    const id = nextRequestId++

    // Copy the bytes into a standalone ArrayBuffer for transfer
    const transferBuffer = bytes.buffer.slice(
      bytes.byteOffset,
      bytes.byteOffset + bytes.byteLength,
    )

    const handler = (event: MessageEvent) => {
      if (event.data.id !== id) return
      worker.removeEventListener('message', handler)
      worker.removeEventListener('error', errHandler)

      if (event.data.error) {
        reject(new Error(event.data.error))
        return
      }

      // Reconstruct the TypedArray from the transferred buffer
      const Ctr = get_ctr(meta.data_type) as unknown as {
        new (buffer: ArrayBuffer, byteOffset: number, length: number): TypedArray<D>
        BYTES_PER_ELEMENT: number
      }
      const data = new Ctr(
        event.data.data,
        0,
        event.data.data.byteLength / Ctr.BYTES_PER_ELEMENT,
      )
      resolve({
        data,
        shape: event.data.shape,
        stride: event.data.stride,
      })
    }

    const errHandler = (err: ErrorEvent) => {
      worker.removeEventListener('message', handler)
      worker.removeEventListener('error', errHandler)
      reject(new Error(err.message ?? 'Worker error'))
    }

    worker.addEventListener('message', handler)
    worker.addEventListener('error', errHandler)
    worker.postMessage(
      { type: 'decode', id, bytes: transferBuffer, meta },
      [transferBuffer],
    )
  })
}

/**
 * Send chunk data to a codec worker for encoding and return the encoded bytes.
 *
 * @param worker - The worker to send the message to.
 * @param data   - The TypedArray chunk data (will be copied and transferred).
 * @param meta   - Codec metadata for reconstructing the pipeline in the worker.
 * @returns Encoded bytes as Uint8Array.
 */
export function workerEncode<D extends DataType>(
  worker: Worker,
  data: TypedArray<D>,
  meta: CodecChunkMeta,
): Promise<Uint8Array> {
  return new Promise((resolve, reject) => {
    const id = nextRequestId++

    // Copy to a standalone ArrayBuffer for transfer
    const view = data as unknown as {
      buffer: ArrayBuffer
      byteOffset: number
      byteLength: number
    }
    const transferBuffer = view.buffer.slice(
      view.byteOffset,
      view.byteOffset + view.byteLength,
    )

    const handler = (event: MessageEvent) => {
      if (event.data.id !== id) return
      worker.removeEventListener('message', handler)
      worker.removeEventListener('error', errHandler)

      if (event.data.error) {
        reject(new Error(event.data.error))
        return
      }

      resolve(new Uint8Array(event.data.bytes))
    }

    const errHandler = (err: ErrorEvent) => {
      worker.removeEventListener('message', handler)
      worker.removeEventListener('error', errHandler)
      reject(new Error(err.message ?? 'Worker error'))
    }

    worker.addEventListener('message', handler)
    worker.addEventListener('error', errHandler)
    worker.postMessage(
      { type: 'encode', id, data: transferBuffer, meta },
      [transferBuffer],
    )
  })
}
