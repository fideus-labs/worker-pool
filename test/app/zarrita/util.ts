/**
 * Slice / range / stride utilities — ported from zarrita.js
 * packages/zarrita/src/indexing/util.ts and packages/zarrita/src/util.ts
 */

import type { ChunkQueue, Indices, Slice } from './types.js'

// ---------------------------------------------------------------------------
// slice() — Python-like slice constructor
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

// ---------------------------------------------------------------------------
// slice_indices() — resolve a Slice to concrete [start, stop, step]
// Ported from CPython's sliceobject.c
// ---------------------------------------------------------------------------

export function slice_indices(
  { start, stop, step }: Slice,
  length: number,
): Indices {
  if (step === 0) {
    throw new Error('slice step cannot be zero')
  }
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
// range() — Python-like range generator
// ---------------------------------------------------------------------------

export function* range(
  start: number,
  stop?: number,
  step = 1,
): Iterable<number> {
  if (stop === undefined) {
    stop = start
    start = 0
  }
  for (let i = start; i < stop; i += step) {
    yield i
  }
}

// ---------------------------------------------------------------------------
// product() — cartesian product generator
// ---------------------------------------------------------------------------

export function* product<T extends Array<Iterable<unknown>>>(
  ...iterables: T
): IterableIterator<{
  [K in keyof T]: T[K] extends Iterable<infer U> ? U : never
}> {
  if (iterables.length === 0) {
    return
  }
  const iterators = iterables.map((it) => it[Symbol.iterator]())
  const results = iterators.map((it) => it.next())
  if (results.some((r) => r.done)) {
    throw new Error('Input contains an empty iterator.')
  }
  for (let i = 0; ; ) {
    if (results[i].done) {
      iterators[i] = iterables[i][Symbol.iterator]()
      results[i] = iterators[i].next()
      if (++i >= iterators.length) {
        return
      }
    } else {
      // @ts-expect-error - TS can't infer mapped tuple types here
      yield results.map(({ value }) => value)
      i = 0
    }
    results[i] = iterators[i].next()
  }
}

// ---------------------------------------------------------------------------
// get_strides() — compute strides for C or F order
// ---------------------------------------------------------------------------

export function get_strides(
  shape: readonly number[],
  order: 'C' | 'F' = 'C',
): number[] {
  const rank = shape.length
  const perm =
    order === 'C'
      ? Array.from({ length: rank }, (_, i) => i)
      : Array.from({ length: rank }, (_, i) => rank - 1 - i)

  let step = 1
  const stride = new Array<number>(rank)
  for (let i = perm.length - 1; i >= 0; i--) {
    stride[perm[i]] = step
    step *= shape[perm[i]]
  }
  return stride
}

// ---------------------------------------------------------------------------
// get_ctr() — map DataType string to TypedArray constructor
// ---------------------------------------------------------------------------

export function get_ctr(dtype: string): {
  new (len: number): ArrayLike<number> & { buffer: ArrayBufferLike; byteOffset: number; byteLength: number; BYTES_PER_ELEMENT: number; fill(v: number): unknown; set(src: ArrayLike<number>, offset?: number): void; subarray(begin: number, end?: number): ArrayLike<number> & { buffer: ArrayBufferLike; byteOffset: number; byteLength: number; BYTES_PER_ELEMENT: number } }
  new (buf: ArrayBufferLike, offset?: number, len?: number): ArrayLike<number> & { buffer: ArrayBufferLike; byteOffset: number; byteLength: number; BYTES_PER_ELEMENT: number }
  BYTES_PER_ELEMENT: number
} {
  const map: Record<string, unknown> = {
    int8: Int8Array,
    int16: Int16Array,
    int32: Int32Array,
    uint8: Uint8Array,
    uint16: Uint16Array,
    uint32: Uint32Array,
    float32: Float32Array,
    float64: Float64Array,
  }
  const ctr = map[dtype]
  if (!ctr) throw new Error(`Unknown data type: ${dtype}`)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return ctr as any
}

// ---------------------------------------------------------------------------
// create_queue() — default immediate-execution queue (from zarrita)
// ---------------------------------------------------------------------------

export function create_queue(): ChunkQueue {
  const promises: Promise<void>[] = []
  return {
    add: (fn) => { promises.push(fn()) },
    onIdle: () => Promise.all(promises),
  }
}
