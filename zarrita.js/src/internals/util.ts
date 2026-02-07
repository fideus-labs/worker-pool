/**
 * Utility functions ported from zarrita.js/src/util.ts
 *
 * These are self-contained reimplementations that avoid deep imports
 * from zarrita. They handle:
 *   - get_ctr: DataType → TypedArray constructor
 *   - get_strides: shape → C-order/F-order strides
 *   - create_chunk_key_encoder: chunk_key_encoding config → encoder function
 *   - byteswap_inplace: in-place byte swapping for endianness
 */

import type { DataType, TypedArrayConstructor } from 'zarrita'

// ---------------------------------------------------------------------------
// TypedArray constructor lookup
// ---------------------------------------------------------------------------

/**
 * Get the TypedArray constructor for a given DataType.
 * Supports all numeric types; string/object types are not supported in
 * worker-pool codec operations.
 */
export function get_ctr<D extends DataType>(
  data_type: D,
): TypedArrayConstructor<D> {
  // @ts-expect-error: dynamic lookup is correct at runtime
  const ctr: TypedArrayConstructor<D> | undefined = ({
    int8: Int8Array,
    int16: Int16Array,
    int32: Int32Array,
    int64: globalThis.BigInt64Array,
    uint8: Uint8Array,
    uint16: Uint16Array,
    uint32: Uint32Array,
    uint64: globalThis.BigUint64Array,
    float32: Float32Array,
    float64: Float64Array,
  } as Record<string, unknown>)[data_type as string]

  if (!ctr) {
    throw new Error(`Unsupported data_type for worker codec: ${data_type}`)
  }
  return ctr
}

// ---------------------------------------------------------------------------
// Strides
// ---------------------------------------------------------------------------

/**
 * Compute strides for a given shape and order.
 *
 * @param shape - Array dimensions
 * @param order - 'C' for row-major (default), 'F' for column-major
 * @returns Stride array
 */
export function get_strides(
  shape: readonly number[],
  order: 'C' | 'F' | number[] = 'C',
): number[] {
  const rank = shape.length
  let permutation: number[]
  if (typeof order === 'string') {
    permutation =
      order === 'C'
        ? Array.from({ length: rank }, (_, i) => i)
        : Array.from({ length: rank }, (_, i) => rank - 1 - i)
  } else {
    permutation = order
  }

  let step = 1
  const stride = new Array<number>(rank)
  for (let i = permutation.length - 1; i >= 0; i--) {
    stride[permutation[i]] = step
    step *= shape[permutation[i]]
  }
  return stride
}

// ---------------------------------------------------------------------------
// Chunk key encoding
// ---------------------------------------------------------------------------

/**
 * Chunk key encoding configuration from zarr.json metadata.
 */
export interface ChunkKeyEncoding {
  name: 'v2' | 'default'
  configuration?: {
    separator?: '.' | '/'
  }
}

/**
 * Create a function that encodes chunk coordinates to a storage key.
 *
 * @param encoding - The chunk_key_encoding from zarr metadata
 * @returns A function that converts chunk coordinates to a string key
 */
export function create_chunk_key_encoder(
  encoding: ChunkKeyEncoding,
): (chunk_coords: number[]) => string {
  if (encoding.name === 'default') {
    const separator = encoding.configuration?.separator ?? '/'
    return (chunk_coords) => ['c', ...chunk_coords].join(separator)
  }
  if (encoding.name === 'v2') {
    const separator = encoding.configuration?.separator ?? '.'
    return (chunk_coords) => chunk_coords.join(separator) || '0'
  }
  throw new Error(`Unknown chunk key encoding: ${encoding.name}`)
}

// ---------------------------------------------------------------------------
// Byte swapping
// ---------------------------------------------------------------------------

/**
 * In-place byte swapping for typed array data.
 */
export function byteswap_inplace(
  view: Uint8Array,
  bytes_per_element: number,
): void {
  const numFlips = bytes_per_element / 2
  const endByteIndex = bytes_per_element - 1
  let t = 0
  for (let i = 0; i < view.length; i += bytes_per_element) {
    for (let j = 0; j < numFlips; j += 1) {
      t = view[i + j]
      view[i + j] = view[i + endByteIndex - j]
      view[i + endByteIndex - j] = t
    }
  }
}

/**
 * Check if the system is little-endian.
 */
export function system_is_little_endian(): boolean {
  const a = new Uint32Array([0x12345678])
  const b = new Uint8Array(a.buffer, a.byteOffset, a.byteLength)
  return !(b[0] === 0x12)
}
