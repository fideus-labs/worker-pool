/**
 * Synthetic dataset creation for benchmarks.
 *
 * Creates in-memory zarr v3 arrays using zarrita's Map-backed store,
 * pre-populated with random data for consistent benchmarking.
 */

import * as zarr from 'zarrita'
import type { DataType, Chunk, Array as ZarrArray, Readable, Mutable } from 'zarrita'
import type { SyntheticConfig, SupportedDtype } from './types.js'

/**
 * Map from our dtype strings to TypedArray constructors.
 */
const DTYPE_CTR: Record<SupportedDtype, {
  new (len: number): ArrayBufferView & { [i: number]: number; length: number }
  BYTES_PER_ELEMENT: number
}> = {
  uint8: Uint8Array,
  uint16: Uint16Array,
  int32: Int32Array,
  float32: Float32Array,
  float64: Float64Array,
}

/**
 * Create a synthetic in-memory zarr array and pre-fill it with sequential data.
 *
 * Returns both the array and a reference copy of the data for write benchmarks.
 */
export async function createSyntheticArray(config: SyntheticConfig): Promise<{
  arr: ZarrArray<DataType, Readable & Mutable>
  refData: ArrayBufferView
}> {
  const store = zarr.root()
  const arr = await zarr.create(store, {
    shape: config.shape,
    chunk_shape: config.chunkShape,
    data_type: config.dtype as DataType,
  })

  const totalSize = config.shape.reduce((a, b) => a * b, 1)
  const Ctr = DTYPE_CTR[config.dtype]
  const data = new Ctr(totalSize)

  // Fill with sequential values (mod 256 for uint8)
  const mod = config.dtype === 'uint8' ? 256 : config.dtype === 'uint16' ? 65536 : 0
  for (let i = 0; i < totalSize; i++) {
    data[i] = mod > 0 ? i % mod : i * 0.5
  }

  // Compute strides (C order)
  const strides = new Array<number>(config.shape.length)
  strides[config.shape.length - 1] = 1
  for (let i = config.shape.length - 2; i >= 0; i--) {
    strides[i] = strides[i + 1] * config.shape[i + 1]
  }

  // Write the data into the array
  await zarr.set(arr, null, {
    data,
    shape: config.shape.slice(),
    stride: strides,
  } as Chunk<DataType>)

  // Return a copy for write benchmarks
  const refCopy = new Ctr(totalSize)
  for (let i = 0; i < totalSize; i++) {
    refCopy[i] = data[i]
  }

  return { arr: arr as ZarrArray<DataType, Readable & Mutable>, refData: refCopy }
}

/**
 * Format a synthetic config as a human-readable info string.
 */
export function formatSyntheticInfo(config: SyntheticConfig): string {
  const totalBytes = config.shape.reduce((a, b) => a * b, 1) *
    DTYPE_CTR[config.dtype].BYTES_PER_ELEMENT
  const mb = (totalBytes / (1024 * 1024)).toFixed(2)
  const numChunks = config.shape.reduce((total, dim, i) =>
    total * Math.ceil(dim / config.chunkShape[i]), 1)
  return [
    `Shape: [${config.shape.join(', ')}]`,
    `Chunks: [${config.chunkShape.join(', ')}] (${numChunks} total)`,
    `Dtype: ${config.dtype}`,
    `Size: ${mb} MB`,
  ].join('\n')
}
