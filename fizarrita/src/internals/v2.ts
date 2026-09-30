/**
 * Zarr v2 `.zarray` → v3 codec list — ported from zarrita.js/src/util.ts
 * (`v2ToV3ArrayMetadata`, 0.7).
 *
 * zarrita does this conversion when it opens a v2 array, but keeps the result
 * private, so the codec worker needs its own copy that builds the same chain:
 *
 *   transpose (order F) → filters → bytes (big-endian only) → compressor
 *
 * with numcodecs filters and compressors under zarrita's `numcodecs.`
 * registry namespace, and `fixedscaleoffset` translated to the native
 * `scale_offset` + `cast_value` pair.
 */

import { InvalidMetadataError } from 'zarrita'
import type { CodecMetadata } from 'zarrita'

/** The parts of a v2 `.zarray` document that determine the codec chain. */
export interface ArrayMetadataV2 {
  dtype: string
  order?: 'C' | 'F'
  filters?: ({ id: string } & Record<string, unknown>)[] | null
  compressor?: ({ id: string } & Record<string, unknown>) | null
}

const DTYPES: Record<string, string> = {
  b1: 'bool',
  i1: 'int8',
  u1: 'uint8',
  i2: 'int16',
  u2: 'uint16',
  i4: 'int32',
  u4: 'uint32',
  i8: 'int64',
  u8: 'uint64',
  f2: 'float16',
  f4: 'float32',
  f8: 'float64',
}

/** Split a numpy dtype string such as `>f4` into data type and byte order. */
function coerce_dtype(dtype: string): {
  data_type: string
  endian?: 'little' | 'big'
} {
  if (dtype === '|O') return { data_type: 'v2:object' }
  const match = dtype.match(/^([<|>])(.*)$/)
  if (!match) {
    throw new InvalidMetadataError(`Invalid dtype: ${dtype}`)
  }
  const [, endian, rest] = match
  const data_type =
    DTYPES[rest] ??
    (rest.startsWith('S') || rest.startsWith('U') ? `v2:${rest}` : undefined)
  if (!data_type) {
    throw new InvalidMetadataError(`Unsupported or unknown dtype: ${dtype}`)
  }
  if (endian === '|') return { data_type }
  return { data_type, endian: endian === '<' ? 'little' : 'big' }
}

const NUMERIC_DTYPES = new Set([
  'int8',
  'int16',
  'int32',
  'int64',
  'uint8',
  'uint16',
  'uint32',
  'uint64',
  'float16',
  'float32',
  'float64',
])

/**
 * numcodecs `fixedscaleoffset` as the v3 `scale_offset` + `cast_value` pair,
 * which together decode `(enc / scale + offset).astype(dtype)`.
 */
function fixed_scale_offset(
  filter: { id: string } & Record<string, unknown>,
  dtype: string,
): CodecMetadata[] {
  const { scale, offset, astype, dtype: filter_dtype } = filter
  if (
    typeof scale !== 'number' ||
    typeof offset !== 'number' ||
    (astype !== undefined && typeof astype !== 'string') ||
    (filter_dtype !== undefined && typeof filter_dtype !== 'string')
  ) {
    throw new InvalidMetadataError(
      `Invalid fixedscaleoffset filter: ${JSON.stringify(filter)}`,
    )
  }
  const codecs: CodecMetadata[] = [
    { name: 'scale_offset', configuration: { scale, offset } },
  ]
  // `astype` defaults to `dtype` in numcodecs — an identity cast, as is one
  // to the array's own dtype — and then there is nothing to convert.
  const target = (astype ?? filter_dtype) as string | undefined
  if (target !== undefined && target !== dtype) {
    const { data_type } = coerce_dtype(target)
    if (!NUMERIC_DTYPES.has(data_type)) {
      throw new InvalidMetadataError(
        `fixedscaleoffset astype must be a numeric data type, got ${target}`,
      )
    }
    codecs.push({
      name: 'cast_value',
      configuration: {
        data_type,
        // `np.around` rounds half to even.
        rounding: 'nearest-even',
        // numpy's integer-overflow behaviour.
        out_of_range: 'wrap',
      },
    })
  }
  return codecs
}

/**
 * The v3 codec list zarrita decodes a v2 array's chunks with.
 *
 * Empty for an uncompressed, unfiltered, native-order little-endian array —
 * the pipeline then falls back to a little-endian `bytes` codec.
 */
export function v2_codecs(meta: ArrayMetadataV2): CodecMetadata[] {
  const codecs: CodecMetadata[] = []
  const dtype = coerce_dtype(meta.dtype)
  if (meta.order === 'F') {
    codecs.push({ name: 'transpose', configuration: { order: 'F' } })
  }
  for (const filter of meta.filters ?? []) {
    if (
      filter.id === 'fixedscaleoffset' ||
      filter.id === 'numcodecs.fixedscaleoffset'
    ) {
      codecs.push(...fixed_scale_offset(filter, meta.dtype))
      continue
    }
    const { id, ...configuration } = filter
    codecs.push({ name: `numcodecs.${id}`, configuration })
  }
  // After any type-changing array-to-array codec (cast_value), so `bytes`
  // is built for the on-disk type.
  if (dtype.endian === 'big') {
    codecs.push({ name: 'bytes', configuration: { endian: 'big' } })
  }
  if (meta.compressor) {
    const { id, ...configuration } = meta.compressor
    codecs.push({ name: `numcodecs.${id}`, configuration })
  }
  return codecs
}
