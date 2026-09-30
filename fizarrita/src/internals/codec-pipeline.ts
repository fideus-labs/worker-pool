/**
 * Codec pipeline builder — adapted from zarrita.js/src/codecs.ts (0.7)
 *
 * Uses zarrita's publicly exported `registry` to load codecs and
 * builds encode/decode pipelines from codec metadata.
 *
 * Self-contained — only imports `registry` and the error classes from
 * zarrita's public API.
 */

import {
  CodecPipelineError,
  InvalidMetadataError,
  registry,
  UnknownCodecError,
} from 'zarrita'
import type { Chunk, CodecMetadata, DataType, Scalar } from 'zarrita'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** The chunk metadata this module's callers describe a pipeline with. */
interface ChunkMetadata<D extends DataType> {
  data_type: D
  shape: number[]
  codecs: CodecMetadata[]
}

/**
 * The chunk metadata zarrita's codecs are configured with — the second
 * argument of every `fromConfig`. zarrita 0.7 renamed it to camelCase and
 * added `fillValue`; a codec reading `meta.dataType` from the 0.6 shape gets
 * `undefined`.
 */
interface CodecChunkMetadata {
  dataType: DataType
  shape: number[]
  codecs: CodecMetadata[]
  fillValue: Scalar<DataType> | null
}

// Codec interfaces — matching zarrita's internal shape
interface CodecEntry {
  fromConfig: (config: unknown, meta: CodecChunkMetadata) => Codec
  kind?: 'array_to_array' | 'array_to_bytes' | 'bytes_to_bytes'
}

interface Codec {
  kind?: string
  encode: (data: unknown) => Promise<unknown> | unknown
  decode: (data: unknown) => Promise<unknown> | unknown
  /**
   * Array-to-array codecs that change the data type (e.g. `cast_value`)
   * describe the metadata after encoding, so the codecs after them —
   * especially `bytes` — are built for the on-disk type.
   */
  getEncodedMeta?: (meta: CodecChunkMetadata) => CodecChunkMetadata
  /**
   * The encoded byte length for a decoded one, when that is a fixed
   * function of the input — `bytes` keeps it, `crc32c` adds four. A shard
   * index is encoded with such codecs only, which is how its length is known
   * before it is fetched.
   */
  computeEncodedSize?: (decodedSize: number) => number
}

interface ArrayToArrayCodec<D extends DataType> {
  encode: (data: Chunk<D>) => Promise<Chunk<D>> | Chunk<D>
  decode: (data: Chunk<D>) => Promise<Chunk<D>> | Chunk<D>
}

interface ArrayToBytesCodec<D extends DataType> {
  encode: (data: Chunk<D>) => Promise<Uint8Array> | Uint8Array
  decode: (data: Uint8Array) => Promise<Chunk<D>> | Chunk<D>
  computeEncodedSize?: (decodedSize: number) => number
}

interface BytesToBytesCodec {
  encode: (data: Uint8Array) => Promise<Uint8Array>
  decode: (data: Uint8Array) => Promise<Uint8Array>
  computeEncodedSize?: (decodedSize: number) => number
}

type Named<T> = { name: string; codec: T }

// ---------------------------------------------------------------------------
// Load codecs from registry
// ---------------------------------------------------------------------------

async function load_codecs<D extends DataType>(chunk_meta: ChunkMetadata<D>) {
  const promises = chunk_meta.codecs.map(async (meta) => {
    const factory = registry.get(meta.name)
    if (!factory) throw new UnknownCodecError(meta.name)
    const CodecClass = await factory()
    return { CodecClass: CodecClass as unknown as CodecEntry, meta }
  })

  const array_to_array: Named<ArrayToArrayCodec<D>>[] = []
  let array_to_bytes: Named<ArrayToBytesCodec<D>> | undefined
  const bytes_to_bytes: Named<BytesToBytesCodec>[] = []

  // The data type seen by each codec. Array-to-array codecs like cast_value
  // change it between the array's declared type and what is stored, and the
  // codecs after them must be built for the stored type. The fill value is
  // not needed to encode or decode a chunk — the caller fills missing chunks
  // itself — so it is not shipped to the worker.
  let current_meta: CodecChunkMetadata = {
    dataType: chunk_meta.data_type,
    shape: chunk_meta.shape,
    codecs: chunk_meta.codecs,
    fillValue: null,
  }

  for await (const { CodecClass, meta } of promises) {
    const codec = CodecClass.fromConfig(meta.configuration, current_meta)
    switch (codec.kind) {
      case 'array_to_array':
        array_to_array.push({
          name: meta.name,
          codec: codec as unknown as ArrayToArrayCodec<D>,
        })
        if (codec.getEncodedMeta) {
          current_meta = codec.getEncodedMeta(current_meta)
        }
        break
      case 'array_to_bytes':
        array_to_bytes = {
          name: meta.name,
          codec: codec as unknown as ArrayToBytesCodec<D>,
        }
        break
      default:
        bytes_to_bytes.push({
          name: meta.name,
          codec: codec as unknown as BytesToBytesCodec,
        })
    }
  }

  if (!array_to_bytes) {
    // No explicit array_to_bytes codec (v2 metadata): zarrita's own
    // little-endian `bytes` codec, built for the type the chain ends on.
    if (
      current_meta.dataType === 'v2:object' ||
      current_meta.dataType === 'string'
    ) {
      throw new InvalidMetadataError(
        `Cannot encode ${current_meta.dataType} to bytes without a codec`,
      )
    }
    const BytesCodec = (await registry.get('bytes')!()) as unknown as CodecEntry
    array_to_bytes = {
      name: 'bytes',
      codec: BytesCodec.fromConfig(
        { endian: 'little' },
        current_meta,
      ) as unknown as ArrayToBytesCodec<D>,
    }
  }

  return { array_to_array, array_to_bytes, bytes_to_bytes }
}

function encoded_size(
  name: string,
  codec: { computeEncodedSize?: (n: number) => number },
  size: number,
): number {
  if (!codec.computeEncodedSize) {
    throw new InvalidMetadataError(
      `Codec "${name}" cannot compute its encoded size; it is not a ` +
        `fixed-size codec and cannot be used in a sharding index pipeline`,
    )
  }
  return codec.computeEncodedSize(size)
}

/**
 * Run one codec step, reporting a failure as zarrita does: a
 * `CodecPipelineError` naming the direction and codec, with the codec's own
 * error as its `cause`.
 */
async function run_step<T>(
  direction: 'encode' | 'decode',
  codec: string,
  fn: () => Promise<T> | T,
): Promise<T> {
  try {
    return await fn()
  } catch (cause) {
    throw new CodecPipelineError({ direction, codec, cause })
  }
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
 * Failures are zarrita's structured errors: an unregistered codec rejects
 * with `UnknownCodecError`, and a codec that throws with `CodecPipelineError`.
 *
 * @param chunk_metadata - The data_type, chunk_shape, and codecs array
 * @returns An object with encode, decode and computeEncodedSize methods
 */
export function create_codec_pipeline<D extends DataType>(
  chunk_metadata: ChunkMetadata<D>,
): {
  encode(chunk: Chunk<D>): Promise<Uint8Array>
  decode(bytes: Uint8Array): Promise<Chunk<D>>
  /**
   * The encoded length of `decodedSize` bytes through this pipeline. Only
   * defined when every byte-producing codec has a fixed output size (see
   * {@link Codec.computeEncodedSize}); rejects with `InvalidMetadataError`
   * otherwise.
   */
  computeEncodedSize(decodedSize: number): Promise<number>
} {
  // Shared by both methods, so concurrent first calls load the codecs once.
  let codecs_promise: ReturnType<typeof load_codecs<D>> | undefined
  const get_codecs = () => {
    if (!codecs_promise) codecs_promise = load_codecs(chunk_metadata)
    return codecs_promise
  }

  return {
    async encode(chunk: Chunk<D>): Promise<Uint8Array> {
      const codecs = await get_codecs()
      for (const { name, codec } of codecs.array_to_array) {
        chunk = await run_step('encode', name, () => codec.encode(chunk))
      }
      const { name, codec } = codecs.array_to_bytes
      let bytes = await run_step('encode', name, () => codec.encode(chunk))
      for (const { name, codec } of codecs.bytes_to_bytes) {
        bytes = await run_step('encode', name, () => codec.encode(bytes))
      }
      return bytes
    },
    async decode(bytes: Uint8Array): Promise<Chunk<D>> {
      const codecs = await get_codecs()
      for (let i = codecs.bytes_to_bytes.length - 1; i >= 0; i--) {
        const { name, codec } = codecs.bytes_to_bytes[i]
        bytes = await run_step('decode', name, () => codec.decode(bytes))
      }
      const { name, codec } = codecs.array_to_bytes
      let chunk = await run_step('decode', name, () => codec.decode(bytes))
      for (let i = codecs.array_to_array.length - 1; i >= 0; i--) {
        const { name, codec } = codecs.array_to_array[i]
        chunk = await run_step('decode', name, () => codec.decode(chunk))
      }
      return chunk
    },
    async computeEncodedSize(decodedSize: number): Promise<number> {
      const codecs = await get_codecs()
      const { name, codec } = codecs.array_to_bytes
      let size = encoded_size(name, codec, decodedSize)
      for (const { name, codec } of codecs.bytes_to_bytes) {
        size = encoded_size(name, codec, size)
      }
      return size
    },
  }
}
