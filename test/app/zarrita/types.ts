/**
 * Simplified zarrita-compatible types for testing.
 * Ported from zarrita.js packages/zarrita/src/indexing/types.ts
 * and packages/zarrita/src/metadata.ts.
 */

// ---------------------------------------------------------------------------
// Data types (numeric subset only)
// ---------------------------------------------------------------------------

export type DataType =
  | 'int8'
  | 'int16'
  | 'int32'
  | 'uint8'
  | 'uint16'
  | 'uint32'
  | 'float32'
  | 'float64'

export type Scalar<_D extends DataType> = number

// biome-ignore format: keep map readable
export type TypedArrayFor<D extends DataType> =
  D extends 'int8' ? Int8Array :
  D extends 'int16' ? Int16Array :
  D extends 'int32' ? Int32Array :
  D extends 'uint8' ? Uint8Array :
  D extends 'uint16' ? Uint16Array :
  D extends 'uint32' ? Uint32Array :
  D extends 'float32' ? Float32Array :
  D extends 'float64' ? Float64Array :
  never

export interface TypedArrayConstructor<D extends DataType> {
  new (length: number): TypedArrayFor<D>
  new (buffer: ArrayBufferLike, byteOffset?: number, length?: number): TypedArrayFor<D>
  new (array: ArrayLike<number>): TypedArrayFor<D>
  readonly BYTES_PER_ELEMENT: number
}

// ---------------------------------------------------------------------------
// Chunk / ndarray representation
// ---------------------------------------------------------------------------

export interface Chunk<D extends DataType> {
  data: TypedArrayFor<D>
  shape: number[]
  stride: number[]
}

// ---------------------------------------------------------------------------
// Indexing types
// ---------------------------------------------------------------------------

/** Resolved index triple: [start, stop, step] — all concrete numbers. */
export type Indices = [start: number, stop: number, step: number]

/** A slice with nullable start/stop/step (like Python's slice). */
export interface Slice {
  start: number | null
  stop: number | null
  step: number | null
}

/**
 * Projection describes how one dimension maps between chunk-local and
 * output indices.
 */
export type Projection =
  | { from: null; to: number }      // integer selection (dim collapse, source side)
  | { from: number; to: null }      // integer selection (dim collapse, dest side)
  | { from: Indices; to: Indices }  // slice-to-slice mapping

// ---------------------------------------------------------------------------
// Queue interface (ChunkQueue — compatible with p-queue)
// ---------------------------------------------------------------------------

export interface ChunkQueue {
  add(fn: () => Promise<void>): void
  onIdle(): Promise<Array<void>>
}

// ---------------------------------------------------------------------------
// Options passed to get / set
// ---------------------------------------------------------------------------

export interface GetOptions {
  create_queue?: () => ChunkQueue
}

export interface SetOptions {
  create_queue?: () => ChunkQueue
}

// ---------------------------------------------------------------------------
// Setter interface (operations on Chunk)
// ---------------------------------------------------------------------------

export type Prepare<D extends DataType> = (
  data: TypedArrayFor<D>,
  shape: number[],
  stride: number[],
) => Chunk<D>

export type SetScalar<D extends DataType> = (
  target: Chunk<D>,
  selection: (Indices | number)[],
  value: Scalar<D>,
) => void

export type SetFromChunk<D extends DataType> = (
  dest: Chunk<D>,
  src: Chunk<D>,
  projections: Projection[],
) => void

export interface Setter<D extends DataType> {
  prepare: Prepare<D>
  set_from_chunk: SetFromChunk<D>
  set_scalar: SetScalar<D>
}

// ---------------------------------------------------------------------------
// Codec worker message protocol
// ---------------------------------------------------------------------------

export interface DecodeRequest {
  type: 'decode'
  id: number
  bytes: ArrayBuffer
  dtype: DataType
  shape: number[]
  delay?: number
}

export interface DecodeResponse {
  type: 'decoded'
  id: number
  data: ArrayBuffer
  shape: number[]
  stride: number[]
}

export interface EncodeRequest {
  type: 'encode'
  id: number
  data: ArrayBuffer
  dtype: DataType
  shape: number[]
  delay?: number
}

export interface EncodeResponse {
  type: 'encoded'
  id: number
  bytes: ArrayBuffer
}

export type CodecRequest = DecodeRequest | EncodeRequest
export type CodecResponse = DecodeResponse | EncodeResponse
