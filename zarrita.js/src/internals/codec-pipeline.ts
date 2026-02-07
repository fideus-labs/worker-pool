/**
 * Codec pipeline builder — adapted from zarrita.js/src/codecs.ts
 *
 * Uses zarrita's publicly exported `registry` to load codecs and
 * builds encode/decode pipelines from codec metadata.
 *
 * Self-contained — only imports `registry` from zarrita's public API.
 */

import { registry } from 'zarrita'
import type { Chunk, CodecMetadata, DataType } from 'zarrita'
import { get_ctr, get_strides } from './util.js'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ChunkMetadata<D extends DataType> {
  data_type: D
  shape: number[]
  codecs: CodecMetadata[]
}

// Codec interfaces — matching zarrita's internal shape
interface CodecEntry {
  fromConfig: (config: unknown, meta: ChunkMetadata<DataType>) => Codec
  kind?: 'array_to_array' | 'array_to_bytes' | 'bytes_to_bytes'
}

interface Codec {
  kind?: string
  encode: (data: unknown) => Promise<unknown> | unknown
  decode: (data: unknown) => Promise<unknown> | unknown
}

interface ArrayToArrayCodec<D extends DataType> {
  encode: (data: Chunk<D>) => Promise<Chunk<D>> | Chunk<D>
  decode: (data: Chunk<D>) => Promise<Chunk<D>> | Chunk<D>
}

interface ArrayToBytesCodec<D extends DataType> {
  encode: (data: Chunk<D>) => Promise<Uint8Array> | Uint8Array
  decode: (data: Uint8Array) => Promise<Chunk<D>> | Chunk<D>
}

interface BytesToBytesCodec {
  encode: (data: Uint8Array) => Promise<Uint8Array>
  decode: (data: Uint8Array) => Promise<Uint8Array>
}

// ---------------------------------------------------------------------------
// Fallback BytesCodec
// ---------------------------------------------------------------------------

const LITTLE_ENDIAN_OS = (() => {
  const a = new Uint32Array([0x12345678])
  const b = new Uint8Array(a.buffer, a.byteOffset, a.byteLength)
  return !(b[0] === 0x12)
})()

function byteswap_inplace(view: Uint8Array, bytes_per_element: number) {
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
 * Fallback BytesCodec for when no explicit array_to_bytes codec is specified.
 * Handles TypedArray <-> Uint8Array conversion with optional endian swap.
 */
function createBytesCodec<D extends DataType>(
  endian: 'little' | 'big' | undefined,
  meta: ChunkMetadata<D>,
): ArrayToBytesCodec<D> {
  const Ctr = get_ctr(meta.data_type)
  const shape = meta.shape
  const stride = get_strides(shape, 'C')
  const sample = new (Ctr as unknown as { new (n: number): { BYTES_PER_ELEMENT: number } })(0)
  const BYTES_PER_ELEMENT = sample.BYTES_PER_ELEMENT

  return {
    encode(chunk: Chunk<D>): Uint8Array {
      const bytes = new Uint8Array(
        (chunk.data as unknown as { buffer: ArrayBuffer }).buffer,
        (chunk.data as unknown as { byteOffset: number }).byteOffset,
        (chunk.data as unknown as { byteLength: number }).byteLength,
      )
      if (LITTLE_ENDIAN_OS && endian === 'big') {
        byteswap_inplace(bytes, BYTES_PER_ELEMENT)
      }
      return bytes
    },
    decode(bytes: Uint8Array): Chunk<D> {
      if (LITTLE_ENDIAN_OS && endian === 'big') {
        byteswap_inplace(bytes, BYTES_PER_ELEMENT)
      }
      return {
        data: new (Ctr as unknown as { new (buf: ArrayBuffer, off: number, len: number): unknown })(
          bytes.buffer as ArrayBuffer,
          bytes.byteOffset,
          bytes.byteLength / BYTES_PER_ELEMENT,
        ) as Chunk<D>['data'],
        shape,
        stride,
      }
    },
  }
}

// ---------------------------------------------------------------------------
// Load codecs from registry
// ---------------------------------------------------------------------------

async function load_codecs<D extends DataType>(chunk_meta: ChunkMetadata<D>) {
  const promises = chunk_meta.codecs.map(async (meta) => {
    const factory = registry.get(meta.name)
    if (!factory) throw new Error(`Unknown codec: ${meta.name}`)
    const CodecClass = await factory()
    return { CodecClass: CodecClass as unknown as CodecEntry, meta }
  })

  const array_to_array: ArrayToArrayCodec<D>[] = []
  let array_to_bytes: ArrayToBytesCodec<D> | undefined
  const bytes_to_bytes: BytesToBytesCodec[] = []

  for await (const { CodecClass, meta } of promises) {
    const codec = CodecClass.fromConfig(meta.configuration, chunk_meta)
    switch (codec.kind) {
      case 'array_to_array':
        array_to_array.push(codec as unknown as ArrayToArrayCodec<D>)
        break
      case 'array_to_bytes':
        array_to_bytes = codec as unknown as ArrayToBytesCodec<D>
        break
      default:
        bytes_to_bytes.push(codec as unknown as BytesToBytesCodec)
    }
  }

  if (!array_to_bytes) {
    // Default BytesCodec for numeric types
    array_to_bytes = createBytesCodec('little', chunk_meta)
  }

  return { array_to_array, array_to_bytes, bytes_to_bytes }
}

// ---------------------------------------------------------------------------
// create_codec_pipeline
// ---------------------------------------------------------------------------

/**
 * Create a codec pipeline from chunk metadata.
 *
 * Uses zarrita's publicly exported `registry` to resolve codec implementations.
 * Lazily loads codecs on first encode/decode call, then caches them.
 *
 * @param chunk_metadata - The data_type, chunk_shape, and codecs array
 * @returns An object with encode and decode methods
 */
export function create_codec_pipeline<D extends DataType>(
  chunk_metadata: ChunkMetadata<D>,
): {
  encode(chunk: Chunk<D>): Promise<Uint8Array>
  decode(bytes: Uint8Array): Promise<Chunk<D>>
} {
  let codecs: Awaited<ReturnType<typeof load_codecs<D>>>

  return {
    async encode(chunk: Chunk<D>): Promise<Uint8Array> {
      if (!codecs) codecs = await load_codecs(chunk_metadata)
      for (const codec of codecs.array_to_array) {
        chunk = await codec.encode(chunk)
      }
      let bytes = await codecs.array_to_bytes.encode(chunk)
      for (const codec of codecs.bytes_to_bytes) {
        bytes = await codec.encode(bytes)
      }
      return bytes
    },
    async decode(bytes: Uint8Array): Promise<Chunk<D>> {
      if (!codecs) codecs = await load_codecs(chunk_metadata)
      for (let i = codecs.bytes_to_bytes.length - 1; i >= 0; i--) {
        bytes = await codecs.bytes_to_bytes[i].decode(bytes)
      }
      let chunk = await codecs.array_to_bytes.decode(bytes)
      for (let i = codecs.array_to_array.length - 1; i >= 0; i--) {
        chunk = await codecs.array_to_array[i].decode(chunk)
      }
      return chunk
    },
  }
}
