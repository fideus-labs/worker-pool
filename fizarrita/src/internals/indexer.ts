/**
 * BasicIndexer — ported from zarrita.js/src/indexing/indexer.ts
 *
 * Handles multi-dimensional selection (slices, integer indices) and iterates
 * over all chunk projections, yielding { chunk_coords, mapping } for each
 * chunk that overlaps the selection.
 *
 * Self-contained — no deep imports from zarrita.
 */

import type { Indices, Slice } from 'zarrita'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export class IndexError extends Error {
  constructor(msg: string) {
    super(msg)
    this.name = 'IndexError'
  }
}

function err_too_many_indices(
  selection: (number | Slice)[],
  shape: readonly number[],
): never {
  throw new IndexError(
    `too many indicies for array; expected ${shape.length}, got ${selection.length}`,
  )
}

function err_boundscheck(dim_len: number): never {
  throw new IndexError(
    `index out of bounds for dimension with length ${dim_len}`,
  )
}

function err_negative_step(): never {
  throw new IndexError('only slices with step >= 1 are supported')
}

function check_selection_length(
  selection: (number | Slice)[],
  shape: readonly number[],
): void {
  if (selection.length > shape.length) {
    err_too_many_indices(selection, shape)
  }
}

// ---------------------------------------------------------------------------
// Slice utilities
// ---------------------------------------------------------------------------

export function slice(stop: number | null): Slice
export function slice(
  start: number | null,
  stop?: number | null,
  step?: number | null,
): Slice
export function slice(
  start: number | null,
  stop?: number | null,
  step: number | null = null,
): Slice {
  if (stop === undefined) {
    stop = start
    start = null
  }
  return { start, stop, step }
}

/**
 * Resolve a Slice to concrete [start, stop, step] given dimension length.
 * Matches Python's slice.indices().
 */
export function slice_indices(
  { start, stop, step }: Slice,
  length: number,
): Indices {
  if (step === 0) throw new Error('slice step cannot be zero')
  step = step ?? 1
  const step_is_negative = step < 0
  const [lower, upper] = step_is_negative ? [-1, length - 1] : [0, length]

  if (start === null) {
    start = step_is_negative ? upper : lower
  } else {
    if (start < 0) {
      start += length
      if (start < lower) start = lower
    } else if (start > upper) {
      start = upper
    }
  }

  if (stop === null) {
    stop = step_is_negative ? lower : upper
  } else {
    if (stop < 0) {
      stop += length
      if (stop < lower) stop = lower
    } else if (stop > upper) {
      stop = upper
    }
  }

  return [start, stop, step]
}

// ---------------------------------------------------------------------------
// Range / Product generators
// ---------------------------------------------------------------------------

function* range(start: number, stop?: number, step = 1): Iterable<number> {
  if (stop === undefined) {
    stop = start
    start = 0
  }
  for (let i = start; i < stop; i += step) {
    yield i
  }
}

function* product<T extends Iterable<unknown>[]>(
  ...iterables: T
): IterableIterator<unknown[]> {
  if (iterables.length === 0) return
  const iterators = iterables.map((it) => it[Symbol.iterator]())
  const results = iterators.map((it) => it.next())
  if (results.some((r) => r.done)) {
    throw new Error('Input contains an empty iterator.')
  }
  for (let i = 0; ; ) {
    if (results[i].done) {
      iterators[i] = iterables[i][Symbol.iterator]()
      results[i] = iterators[i].next()
      if (++i >= iterators.length) return
    } else {
      yield results.map(({ value }) => value)
      i = 0
    }
    results[i] = iterators[i].next()
  }
}

// ---------------------------------------------------------------------------
// normalize_integer_selection
// ---------------------------------------------------------------------------

export function normalize_integer_selection(
  dim_sel: number,
  dim_len: number,
): number {
  dim_sel = Math.trunc(dim_sel)
  if (dim_sel < 0) dim_sel = dim_len + dim_sel
  if (dim_sel >= dim_len || dim_sel < 0) err_boundscheck(dim_len)
  return dim_sel
}

// ---------------------------------------------------------------------------
// IntDimIndexer
// ---------------------------------------------------------------------------

interface IntChunkDimProjection {
  dim_chunk_ix: number
  dim_chunk_sel: number
}

class IntDimIndexer {
  dim_sel: number
  dim_len: number
  dim_chunk_len: number
  nitems: 1 = 1

  constructor(opts: { dim_sel: number; dim_len: number; dim_chunk_len: number }) {
    this.dim_sel = normalize_integer_selection(opts.dim_sel, opts.dim_len)
    this.dim_len = opts.dim_len
    this.dim_chunk_len = opts.dim_chunk_len
  }

  *[Symbol.iterator](): IterableIterator<IntChunkDimProjection> {
    const dim_chunk_ix = Math.floor(this.dim_sel / this.dim_chunk_len)
    const dim_offset = dim_chunk_ix * this.dim_chunk_len
    const dim_chunk_sel = this.dim_sel - dim_offset
    yield { dim_chunk_ix, dim_chunk_sel }
  }
}

// ---------------------------------------------------------------------------
// SliceDimIndexer
// ---------------------------------------------------------------------------

interface SliceChunkDimProjection {
  dim_chunk_ix: number
  dim_chunk_sel: Indices
  dim_out_sel: Indices
}

class SliceDimIndexer {
  start: number
  stop: number
  step: number
  dim_len: number
  dim_chunk_len: number
  nitems: number
  nchunks: number

  constructor(opts: { dim_sel: Slice; dim_len: number; dim_chunk_len: number }) {
    const [start, stop, step] = slice_indices(opts.dim_sel, opts.dim_len)
    this.start = start
    this.stop = stop
    this.step = step
    if (this.step < 1) err_negative_step()
    this.dim_len = opts.dim_len
    this.dim_chunk_len = opts.dim_chunk_len
    this.nitems = Math.max(0, Math.ceil((this.stop - this.start) / this.step))
    this.nchunks = Math.ceil(this.dim_len / this.dim_chunk_len)
  }

  *[Symbol.iterator](): IterableIterator<SliceChunkDimProjection> {
    const dim_chunk_ix_from = Math.floor(this.start / this.dim_chunk_len)
    const dim_chunk_ix_to = Math.ceil(this.stop / this.dim_chunk_len)
    for (const dim_chunk_ix of range(dim_chunk_ix_from, dim_chunk_ix_to)) {
      const dim_offset = dim_chunk_ix * this.dim_chunk_len
      const dim_limit = Math.min(
        this.dim_len,
        (dim_chunk_ix + 1) * this.dim_chunk_len,
      )
      const dim_chunk_len = dim_limit - dim_offset

      let dim_out_offset = 0
      let dim_chunk_sel_start = 0
      if (this.start < dim_offset) {
        const remainder = (dim_offset - this.start) % this.step
        if (remainder) dim_chunk_sel_start += this.step - remainder
        dim_out_offset = Math.ceil((dim_offset - this.start) / this.step)
      } else {
        dim_chunk_sel_start = this.start - dim_offset
      }
      const dim_chunk_sel_stop =
        this.stop > dim_limit ? dim_chunk_len : this.stop - dim_offset

      const dim_chunk_sel: Indices = [
        dim_chunk_sel_start,
        dim_chunk_sel_stop,
        this.step,
      ]
      const dim_chunk_nitems = Math.ceil(
        (dim_chunk_sel_stop - dim_chunk_sel_start) / this.step,
      )
      const dim_out_sel: Indices = [
        dim_out_offset,
        dim_out_offset + dim_chunk_nitems,
        1,
      ]
      yield { dim_chunk_ix, dim_chunk_sel, dim_out_sel }
    }
  }
}

// ---------------------------------------------------------------------------
// normalize_selection
// ---------------------------------------------------------------------------

export function normalize_selection(
  selection: null | (Slice | null | number)[],
  shape: readonly number[],
): (number | Slice)[] {
  let normalized: (number | Slice)[]
  if (selection === null || selection === undefined) {
    normalized = shape.map(() => slice(null))
  } else if (Array.isArray(selection)) {
    normalized = selection.map((s) => s ?? slice(null))
  } else {
    normalized = shape.map(() => slice(null))
  }
  check_selection_length(normalized, shape)
  return normalized
}

// ---------------------------------------------------------------------------
// Projection types
// ---------------------------------------------------------------------------

export type IndexerProjection =
  | { from: number; to: null }
  | { from: Indices; to: Indices }

export interface ChunkProjection {
  chunk_coords: number[]
  mapping: IndexerProjection[]
}

// ---------------------------------------------------------------------------
// BasicIndexer
// ---------------------------------------------------------------------------

export class BasicIndexer {
  dim_indexers: (SliceDimIndexer | IntDimIndexer)[]
  shape: number[]

  constructor(opts: {
    selection: null | (null | number | Slice)[]
    shape: readonly number[]
    chunk_shape: readonly number[]
  }) {
    this.dim_indexers = normalize_selection(opts.selection, opts.shape).map(
      (dim_sel, i) => {
        if (typeof dim_sel === 'number') {
          return new IntDimIndexer({
            dim_sel,
            dim_len: opts.shape[i],
            dim_chunk_len: opts.chunk_shape[i],
          })
        }
        return new SliceDimIndexer({
          dim_sel,
          dim_len: opts.shape[i],
          dim_chunk_len: opts.chunk_shape[i],
        })
      },
    )
    this.shape = this.dim_indexers
      .filter((ixr): ixr is SliceDimIndexer => ixr instanceof SliceDimIndexer)
      .map((sixr) => sixr.nitems)
  }

  *[Symbol.iterator](): IterableIterator<ChunkProjection> {
    for (const dim_projections of product(...this.dim_indexers)) {
      const chunk_coords = (
        dim_projections as (IntChunkDimProjection | SliceChunkDimProjection)[]
      ).map((p) => p.dim_chunk_ix)
      const mapping: IndexerProjection[] = (
        dim_projections as (IntChunkDimProjection | SliceChunkDimProjection)[]
      ).map((p) => {
        if ('dim_out_sel' in p) {
          return { from: p.dim_chunk_sel, to: p.dim_out_sel }
        }
        return { from: p.dim_chunk_sel, to: null }
      })
      yield { chunk_coords, mapping }
    }
  }
}
