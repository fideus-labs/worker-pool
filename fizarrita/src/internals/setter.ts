/**
 * Binary setter — ported from zarrita.js/src/indexing/ops.ts
 *
 * Provides prepare, set_scalar, and set_from_chunk operations on Chunk objects.
 * Works directly on raw TypedArray/Uint8Array memory for performance.
 *
 * Self-contained — no deep imports from zarrita.
 */

import type { Chunk, DataType, Indices, Projection, Scalar, TypedArray } from 'zarrita'

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

export function compat_chunk<D extends DataType>(
  arr: Chunk<D>,
): {
  data: Uint8Array
  stride: number[]
  bytes_per_element: number
} {
  return {
    data: new Uint8Array(
      (arr.data as unknown as { buffer: ArrayBufferLike }).buffer,
      (arr.data as unknown as { byteOffset: number }).byteOffset,
      (arr.data as unknown as { byteLength: number }).byteLength,
    ),
    stride: arr.stride,
    bytes_per_element: (arr.data as unknown as { BYTES_PER_ELEMENT: number }).BYTES_PER_ELEMENT,
  }
}

function compat_scalar<D extends DataType>(
  arr: Chunk<D>,
  value: Scalar<D>,
): Uint8Array {
  const TypedArrayCtor = (arr.data as unknown as { constructor: new (arr: unknown[]) => { buffer: ArrayBuffer; byteOffset: number; byteLength: number } }).constructor
  const data = new TypedArrayCtor([value])
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
}

// ---------------------------------------------------------------------------
// set_scalar_binary
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
// set_from_chunk_binary
// ---------------------------------------------------------------------------

/**
 * Find the selection that is one unbroken run of memory on both sides, which
 * the caller can copy with a single `set` — ported from zarrita 0.7.4.
 *
 * The run exists when each remaining dimension is a step-1 slice and each
 * stride is the product of the lengths inside it, checked from the innermost
 * dimension outward. The test is on the strides, so a transposed chunk fails
 * it and falls to the element-by-element path, which is always correct.
 *
 * Returns the size of the run and its start offset on each side (in
 * elements), or `null` when the caller must recurse.
 */
function contiguous_span(
  projections: Projection[],
  dest_stride: number[],
  src_stride: number[],
): { size: number; dest_offset: number; src_offset: number } | null {
  // A dropped dimension leaves the two sides at different ranks, so the
  // strides no longer line up with the projections. The recursion removes
  // those dimensions first.
  if (
    projections.length !== dest_stride.length ||
    projections.length !== src_stride.length
  ) {
    return null
  }
  let size = 1
  let dest_offset = 0
  let src_offset = 0
  for (let i = projections.length - 1; i >= 0; i--) {
    const proj = projections[i]
    if (proj.from === null || proj.to === null) return null
    if (dest_stride[i] !== size || src_stride[i] !== size) return null
    const [from, to, step] = proj.to as Indices
    const [sfrom, , sstep] = proj.from as Indices
    if (step !== 1 || sstep !== 1) return null
    dest_offset += size * from
    src_offset += size * sfrom
    size *= indices_len(from, to, step)
  }
  return { size, dest_offset, src_offset }
}

export function set_from_chunk_binary(
  dest: { data: Uint8Array; stride: number[] },
  src: { data: Uint8Array; stride: number[] },
  bytes_per_element: number,
  projections: Projection[],
): void {
  const span = contiguous_span(projections, dest.stride, src.stride)
  if (span !== null) {
    const offset = span.src_offset * bytes_per_element
    dest.data.set(
      src.data.subarray(offset, offset + span.size * bytes_per_element),
      span.dest_offset * bytes_per_element,
    )
    return
  }
  const [proj, ...projs] = projections
  const [dstride, ...dstrides] = dest.stride
  const [sstride, ...sstrides] = src.stride

  if (proj.from === null) {
    if (projs.length === 0) {
      // The last axis has stride 1 only in a C-contiguous chunk; a transpose
      // order can put a different axis there. (zarrita 0.7.4, #445.)
      dest.data.set(
        src.data.subarray(0, bytes_per_element),
        dstride * (proj.to as number) * bytes_per_element,
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
      const offset = sstride * (proj.from as number) * bytes_per_element
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
    // Not one run of memory (a contiguous last dimension was caught by
    // contiguous_span above), so copy each element.
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

export const setter = {
  prepare<D extends DataType>(
    data: TypedArray<D>,
    shape: number[],
    stride: number[],
  ): Chunk<D> {
    return { data, shape, stride }
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
