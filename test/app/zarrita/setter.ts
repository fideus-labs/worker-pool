/**
 * Binary setter — full port from zarrita.js
 * packages/zarrita/src/indexing/ops.ts (the setter object and binary helpers)
 */

import type { Chunk, DataType, Indices, Projection, Scalar, Setter } from './types.js'

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function indices_len(start: number, stop: number, step: number): number {
  if (step < 0 && stop < start) {
    return Math.floor((start - stop - 1) / -step) + 1
  }
  if (start < stop) return Math.floor((stop - start - 1) / step) + 1
  return 0
}

function compat_chunk<D extends DataType>(arr: Chunk<D>): {
  data: Uint8Array
  stride: number[]
  bytes_per_element: number
} {
  return {
    data: new Uint8Array(
      arr.data.buffer,
      arr.data.byteOffset,
      arr.data.byteLength,
    ),
    stride: arr.stride,
    bytes_per_element: arr.data.BYTES_PER_ELEMENT,
  }
}

function compat_scalar<D extends DataType>(
  arr: Chunk<D>,
  value: Scalar<D>,
): Uint8Array {
  const TypedArray = arr.data.constructor as {
    new (arr: number[]): typeof arr.data
  }
  const data = new TypedArray([value])
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}

// ---------------------------------------------------------------------------
// set_scalar_binary — recursive dimension-by-dimension scalar fill
// ---------------------------------------------------------------------------

function set_scalar_binary(
  out: { data: Uint8Array; stride: number[] },
  out_selection: (Indices | number)[],
  value: Uint8Array,
  bytes_per_element: number,
): void {
  if (out_selection.length === 0) {
    out.data.set(value, 0)
    return
  }
  const [sel, ...rest] = out_selection
  const [curr_stride, ...stride] = out.stride
  if (typeof sel === 'number') {
    const data = out.data.subarray(curr_stride * sel * bytes_per_element)
    set_scalar_binary({ data, stride }, rest, value, bytes_per_element)
    return
  }
  const [from, to, step] = sel
  const len = indices_len(from, to, step)
  if (rest.length === 0) {
    for (let i = 0; i < len; i++) {
      out.data.set(value, curr_stride * (from + step * i) * bytes_per_element)
    }
    return
  }
  for (let i = 0; i < len; i++) {
    const data = out.data.subarray(
      curr_stride * (from + step * i) * bytes_per_element,
    )
    set_scalar_binary({ data, stride }, rest, value, bytes_per_element)
  }
}

// ---------------------------------------------------------------------------
// set_from_chunk_binary — recursive dimension-by-dimension chunk copy
// ---------------------------------------------------------------------------

function set_from_chunk_binary(
  dest: { data: Uint8Array; stride: number[] },
  src: { data: Uint8Array; stride: number[] },
  bytes_per_element: number,
  projections: Projection[],
): void {
  const [proj, ...projs] = projections
  const [dstride, ...dstrides] = dest.stride
  const [sstride, ...sstrides] = src.stride

  if (proj.from === null) {
    if (projs.length === 0) {
      dest.data.set(
        src.data.subarray(0, bytes_per_element),
        (proj.to as number) * bytes_per_element,
      )
      return
    }
    set_from_chunk_binary(
      {
        data: dest.data.subarray(
          dstride * (proj.to as number) * bytes_per_element,
        ),
        stride: dstrides,
      },
      src,
      bytes_per_element,
      projs,
    )
    return
  }
  if (proj.to === null) {
    if (projs.length === 0) {
      const offset = (proj.from as number) * bytes_per_element
      dest.data.set(src.data.subarray(offset, offset + bytes_per_element), 0)
      return
    }
    set_from_chunk_binary(
      dest,
      {
        data: src.data.subarray(
          sstride * (proj.from as number) * bytes_per_element,
        ),
        stride: sstrides,
      },
      bytes_per_element,
      projs,
    )
    return
  }

  const [from, to, step] = proj.to as Indices
  const [sfrom, , sstep] = proj.from as Indices
  const len = indices_len(from, to, step)

  if (projs.length === 0) {
    // Fast path: contiguous copy
    if (step === 1 && sstep === 1 && dstride === 1 && sstride === 1) {
      const offset = sfrom * bytes_per_element
      const size = len * bytes_per_element
      dest.data.set(
        src.data.subarray(offset, offset + size),
        from * bytes_per_element,
      )
      return
    }
    for (let i = 0; i < len; i++) {
      const offset = sstride * (sfrom + sstep * i) * bytes_per_element
      dest.data.set(
        src.data.subarray(offset, offset + bytes_per_element),
        dstride * (from + step * i) * bytes_per_element,
      )
    }
    return
  }

  for (let i = 0; i < len; i++) {
    set_from_chunk_binary(
      {
        data: dest.data.subarray(
          dstride * (from + i * step) * bytes_per_element,
        ),
        stride: dstrides,
      },
      {
        data: src.data.subarray(
          sstride * (sfrom + i * sstep) * bytes_per_element,
        ),
        stride: sstrides,
      },
      bytes_per_element,
      projs,
    )
  }
}

// ---------------------------------------------------------------------------
// Exported setter object
// ---------------------------------------------------------------------------

export const setter: Setter<DataType> = {
  prepare<D extends DataType>(
    data: Chunk<D>['data'],
    shape: number[],
    stride: number[],
  ): Chunk<D> {
    return { data, shape, stride } as Chunk<D>
  },

  set_scalar<D extends DataType>(
    dest: Chunk<D>,
    sel: (number | Indices)[],
    value: Scalar<D>,
  ): void {
    const view = compat_chunk(dest)
    set_scalar_binary(
      view,
      sel,
      compat_scalar(dest, value),
      view.bytes_per_element,
    )
  },

  set_from_chunk<D extends DataType>(
    dest: Chunk<D>,
    src: Chunk<D>,
    projections: Projection[],
  ): void {
    const view = compat_chunk(dest)
    set_from_chunk_binary(
      view,
      compat_chunk(src),
      view.bytes_per_element,
      projections,
    )
  },
}
