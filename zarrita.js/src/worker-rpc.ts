/**
 * Main-thread helpers for communicating with the codec worker via postMessage.
 *
 * Optimizations over the naive approach:
 *   - Persistent message dispatcher: one listener per worker, routed by request ID
 *   - Meta-init protocol: codec metadata sent once per worker, referenced by ID thereafter
 *   - Zero-copy buffer transfer when the TypedArray already owns its buffer
 */

import type { Chunk, DataType, TypedArray } from 'zarrita'
import { get_ctr } from './internals/util.js'
import type { CodecChunkMeta, Projection } from './types.js'

// ---------------------------------------------------------------------------
// Persistent message dispatcher
// ---------------------------------------------------------------------------

interface PendingRequest {
  resolve: (data: unknown) => void
  reject: (err: Error) => void
}

/**
 * Per-worker dispatcher. Installs a single persistent `message` and `error`
 * listener and routes responses by request ID.
 */
class WorkerDispatcher {
  private pending = new Map<number, PendingRequest>()
  /** Tracks which metaIds have been sent to this worker. */
  private sentMetas = new Set<number>()

  constructor(private worker: Worker) {
    worker.addEventListener('message', this.onMessage)
    worker.addEventListener('error', this.onError)
  }

  private onMessage = (event: MessageEvent): void => {
    const { id } = event.data
    const req = this.pending.get(id)
    if (!req) return
    this.pending.delete(id)

    if (event.data.error) {
      req.reject(new Error(event.data.error))
    } else {
      req.resolve(event.data)
    }
  }

  private onError = (err: ErrorEvent): void => {
    // Reject all pending requests on worker error
    const error = new Error(err.message ?? 'Worker error')
    for (const req of this.pending.values()) {
      req.reject(error)
    }
    this.pending.clear()
  }

  send(id: number, message: unknown, transfer: Transferable[]): Promise<unknown> {
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      this.worker.postMessage(message, transfer)
    })
  }

  hasMeta(metaId: number): boolean {
    return this.sentMetas.has(metaId)
  }

  markMeta(metaId: number): void {
    this.sentMetas.add(metaId)
  }

  destroy(): void {
    this.worker.removeEventListener('message', this.onMessage)
    this.worker.removeEventListener('error', this.onError)
    this.pending.clear()
    this.sentMetas.clear()
  }
}

/** Map from Worker to its dispatcher. WeakMap so dispatchers are GC'd with workers. */
const dispatchers = new WeakMap<Worker, WorkerDispatcher>()

function getDispatcher(worker: Worker): WorkerDispatcher {
  let d = dispatchers.get(worker)
  if (!d) {
    d = new WorkerDispatcher(worker)
    dispatchers.set(worker, d)
  }
  return d
}

// ---------------------------------------------------------------------------
// Meta ID registry — assigns stable IDs to unique codec metadata
// ---------------------------------------------------------------------------

let nextMetaId = 0
const metaKeyToId = new Map<string, number>()
const metaIdToMeta = new Map<number, CodecChunkMeta>()

/**
 * Get or create a stable metaId for the given codec metadata.
 * Uses JSON.stringify as the dedup key — called once per unique array config.
 */
export function getMetaId(meta: CodecChunkMeta): number {
  const key = JSON.stringify(meta)
  let id = metaKeyToId.get(key)
  if (id === undefined) {
    id = nextMetaId++
    metaKeyToId.set(key, id)
    metaIdToMeta.set(id, meta)
  }
  return id
}

// ---------------------------------------------------------------------------
// Request ID
// ---------------------------------------------------------------------------

let nextRequestId = 0

// ---------------------------------------------------------------------------
// Buffer transfer helpers
// ---------------------------------------------------------------------------

/**
 * Prepare a buffer for zero-copy transfer.
 * If the view already owns the full buffer AND we're certain it won't be
 * reused, we can transfer directly. Otherwise, slice to get a standalone copy.
 *
 * @param ownedExclusively - set to true only when the caller created the buffer
 *   and no other code holds a reference (e.g., encode output). Store-fetched
 *   bytes must always be copied because the store may cache the buffer.
 */
function prepareTransferBuffer(
  buffer: ArrayBuffer,
  byteOffset: number,
  byteLength: number,
  ownedExclusively: boolean,
): ArrayBuffer {
  if (
    ownedExclusively &&
    byteOffset === 0 &&
    byteLength === buffer.byteLength
  ) {
    return buffer
  }
  return buffer.slice(byteOffset, byteOffset + byteLength)
}

// ---------------------------------------------------------------------------
// Ensure meta is initialized on the worker
// ---------------------------------------------------------------------------

async function ensureMeta(
  dispatcher: WorkerDispatcher,
  metaId: number,
): Promise<void> {
  if (dispatcher.hasMeta(metaId)) return
  const meta = metaIdToMeta.get(metaId)!
  const id = nextRequestId++
  await dispatcher.send(id, { type: 'init', id, metaId, meta }, [])
  dispatcher.markMeta(metaId)
}

// ---------------------------------------------------------------------------
// workerDecode
// ---------------------------------------------------------------------------

/**
 * Send raw bytes to a codec worker for decoding and return the decoded Chunk.
 */
export async function workerDecode<D extends DataType>(
  worker: Worker,
  bytes: Uint8Array,
  metaId: number,
  meta: CodecChunkMeta,
): Promise<Chunk<D>> {
  const dispatcher = getDispatcher(worker)
  await ensureMeta(dispatcher, metaId)

  const id = nextRequestId++
  // Always copy: store-fetched bytes may be cached/shared
  const transferBuffer = prepareTransferBuffer(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength,
    false,
  )

  const response = await dispatcher.send(
    id,
    { type: 'decode', id, bytes: transferBuffer, metaId },
    [transferBuffer],
  ) as { data: ArrayBuffer; shape: number[]; stride: number[] }

  // Reconstruct the TypedArray from the transferred buffer
  const Ctr = get_ctr(meta.data_type) as unknown as {
    new (buffer: ArrayBuffer, byteOffset: number, length: number): TypedArray<D>
    BYTES_PER_ELEMENT: number
  }
  const data = new Ctr(
    response.data,
    0,
    response.data.byteLength / Ctr.BYTES_PER_ELEMENT,
  )
  return { data, shape: response.shape, stride: response.stride }
}

// ---------------------------------------------------------------------------
// workerEncode
// ---------------------------------------------------------------------------

/**
 * Send chunk data to a codec worker for encoding and return the encoded bytes.
 */
export async function workerEncode<D extends DataType>(
  worker: Worker,
  data: TypedArray<D>,
  metaId: number,
  meta: CodecChunkMeta,
): Promise<Uint8Array> {
  const dispatcher = getDispatcher(worker)
  await ensureMeta(dispatcher, metaId)

  const id = nextRequestId++
  const view = data as unknown as {
    buffer: ArrayBuffer
    byteOffset: number
    byteLength: number
  }
  // Encode data is owned by us — safe to transfer without copy
  const transferBuffer = prepareTransferBuffer(
    view.buffer,
    view.byteOffset,
    view.byteLength,
    true,
  )

  const response = await dispatcher.send(
    id,
    { type: 'encode', id, data: transferBuffer, metaId },
    [transferBuffer],
  ) as { bytes: ArrayBuffer }

  return new Uint8Array(response.bytes)
}

// ---------------------------------------------------------------------------
// workerDecodeInto — decode and write directly into SharedArrayBuffer
// ---------------------------------------------------------------------------

/**
 * Send raw bytes to a codec worker for decoding, with the worker writing
 * the decoded data directly into a SharedArrayBuffer output.
 *
 * This eliminates the transfer-back step and the main-thread copy — the
 * worker decodes the chunk, then runs setter.set_from_chunk to write
 * directly into the shared output memory.
 *
 * Only usable when the output is backed by SharedArrayBuffer.
 */
export async function workerDecodeInto(
  worker: Worker,
  bytes: Uint8Array,
  metaId: number,
  meta: CodecChunkMeta,
  output: SharedArrayBuffer,
  outputByteLength: number,
  outputStride: number[],
  projections: Projection[],
  bytesPerElement: number,
): Promise<void> {
  const dispatcher = getDispatcher(worker)
  await ensureMeta(dispatcher, metaId)

  const id = nextRequestId++
  // Always copy: store-fetched bytes may be cached/shared
  const transferBuffer = prepareTransferBuffer(
    bytes.buffer,
    bytes.byteOffset,
    bytes.byteLength,
    false,
  )

  // SharedArrayBuffer goes in the message but NOT in the transfer list.
  // postMessage shares SABs via structured clone automatically.
  // Only the raw bytes ArrayBuffer is transferred (one-way, neutered on main thread).
  await dispatcher.send(
    id,
    {
      type: 'decode_into' as const,
      id,
      bytes: transferBuffer,
      metaId,
      output,
      outputByteLength,
      outputStride,
      projections,
      bytesPerElement,
    },
    [transferBuffer],
  )
}
