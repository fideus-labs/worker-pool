/**
 * Simplified zarrita-compatible ZarrArray backed by Map<string, Uint8Array>.
 *
 * This is a minimal reimplementation of zarrita's Array class that provides
 * just enough for the get/set indexing tests:
 *   - In-memory Map store
 *   - getChunk() with fill-value fallback
 *   - BytesCodec (raw TypedArray <-> Uint8Array)
 *   - Default chunk key encoding ("c/0/1/...")
 */

import type { Chunk, DataType, Scalar, TypedArrayFor } from './types.js'
import { get_ctr, get_strides } from './util.js'

// ---------------------------------------------------------------------------
// Codec — simple BytesCodec (no compression)
// ---------------------------------------------------------------------------

export interface Codec<D extends DataType> {
  encode(chunk: Chunk<D>): Uint8Array
  decode(bytes: Uint8Array): Chunk<D>
}

function createBytesCodec<D extends DataType>(
  dtype: D,
  chunk_shape: number[],
): Codec<D> {
  const Ctr = get_ctr(dtype)
  const stride = get_strides(chunk_shape)
  return {
    encode(chunk: Chunk<D>): Uint8Array {
      // Create a copy so the source buffer can be transferred/reused
      const src = new Uint8Array(
        chunk.data.buffer,
        chunk.data.byteOffset,
        chunk.data.byteLength,
      )
      const copy = new Uint8Array(src.length)
      copy.set(src)
      return copy
    },
    decode(bytes: Uint8Array): Chunk<D> {
      // Create a TypedArray view over a copy of the bytes
      const buf = new ArrayBuffer(bytes.byteLength)
      new Uint8Array(buf).set(bytes)
      const data = new Ctr(buf, 0, buf.byteLength / Ctr.BYTES_PER_ELEMENT)
      return { data: data as TypedArrayFor<D>, shape: chunk_shape.slice(), stride: stride.slice() }
    },
  }
}

// ---------------------------------------------------------------------------
// Chunk key encoding (default v3: "c/0/1/...")
// ---------------------------------------------------------------------------

function encode_chunk_key(chunk_coords: number[]): string {
  return ['c', ...chunk_coords].join('/')
}

// ---------------------------------------------------------------------------
// ZarrArray
// ---------------------------------------------------------------------------

export interface ZarrArrayOptions<D extends DataType> {
  shape: number[]
  chunk_shape: number[]
  data_type: D
  fill_value?: Scalar<D>
  store?: Map<string, Uint8Array>
}

export class ZarrArray<D extends DataType> {
  readonly store: Map<string, Uint8Array>
  readonly shape: number[]
  readonly chunks: number[]
  readonly dtype: D
  readonly fill_value: Scalar<D>
  readonly codec: Codec<D>
  readonly path: string

  constructor(options: ZarrArrayOptions<D>) {
    this.store = options.store ?? new Map()
    this.shape = options.shape
    this.chunks = options.chunk_shape
    this.dtype = options.data_type
    this.fill_value = options.fill_value ?? (0 as Scalar<D>)
    this.codec = createBytesCodec(options.data_type, options.chunk_shape)
    this.path = '/'
  }

  /** Resolve a relative path against this array's location. */
  resolve(key: string): { path: string } {
    if (this.path === '/') {
      return { path: `/${key}` }
    }
    return { path: `${this.path}/${key}` }
  }

  /** Encode chunk coordinates to a store key. */
  encodeChunkKey(chunk_coords: number[]): string {
    return encode_chunk_key(chunk_coords)
  }

  /** Get strides for a given shape (C-order). */
  getStrides(shape: number[]): number[] {
    return get_strides(shape)
  }

  /**
   * Fetch and decode a chunk. If the chunk doesn't exist in the store,
   * returns a fill-value chunk.
   */
  async getChunk(chunk_coords: number[]): Promise<Chunk<D>> {
    const key = this.resolve(encode_chunk_key(chunk_coords)).path
    const bytes = this.store.get(key)

    if (!bytes) {
      // Chunk not in store — return fill value
      const size = this.chunks.reduce((a, b) => a * b, 1)
      const Ctr = get_ctr(this.dtype)
      const data = new Ctr(size) as TypedArrayFor<D>
      ;(data as unknown as { fill(v: number): void }).fill(this.fill_value)
      return {
        data,
        shape: this.chunks.slice(),
        stride: get_strides(this.chunks),
      }
    }

    return this.codec.decode(bytes)
  }
}
