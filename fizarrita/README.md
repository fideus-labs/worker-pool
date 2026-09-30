# @fideus-labs/fizarrita

Worker-pool-accelerated `get`/`set` for [zarrita.js](https://github.com/manzt/zarrita.js) — offloads codec encode/decode to workers via [@fideus-labs/worker-pool](https://github.com/fideus-labs/worker-pool).

Runs in the browser on Web Workers and in Node on `node:worker_threads`; the
right codec worker is picked for you. See [Node.js](#nodejs).

## Installation

```sh
pnpm add @fideus-labs/fizarrita @fideus-labs/worker-pool zarrita
```

## Quick start

```ts
import { WorkerPool } from '@fideus-labs/worker-pool'
import { getWorker, setWorker } from '@fideus-labs/fizarrita'
import * as zarr from 'zarrita'

const pool = new WorkerPool(navigator.hardwareConcurrency ?? 4)

const store = new zarr.FetchStore('https://example.com/data.zarr')
const arr = await zarr.open(store, { kind: 'array' })

try {
  // Read — codec decode runs on workers
  const chunk = await getWorker(arr, null, { pool })

  // Write — codec encode runs on workers
  await setWorker(arr, null, chunk, { pool })
} finally {
  pool.terminateWorkers()
}
```

## API

### `getWorker(arr, selection, options)`

Drop-in replacement for zarrita's `get`. Fetches raw bytes from the store on
the main thread, transfers them to pooled workers for codec decoding, then
assembles the output chunk.

```ts
const chunk = await getWorker(arr, [zarr.slice(0, 10)], { pool })
// chunk.data  — TypedArray
// chunk.shape — number[]
```

**Options** (`GetWorkerOptions`):

| Option | Type | Default | Description |
|---|---|---|---|
| `pool` | `WorkerPool` | **required** | Worker pool for codec operations |
| `workerUrl` | `string \| URL` | built-in | URL of the codec worker script |
| `opts` | `StoreOpts` | — | Pass-through options for the store's `get` method |
| `useSharedArrayBuffer` | `boolean` | `false` | Allocate output on SharedArrayBuffer with decode-into-shared optimization |
| `cache` | `ChunkCache` | — | Optional decoded-chunk cache to avoid redundant decompression |
| `signal` | `AbortSignal` | — | Aborts the read: cancels in-flight store fetches, drops queued decode tasks |

### `setWorker(arr, selection, value, options)`

Drop-in replacement for zarrita's `set`. Handles partial chunk updates by
decoding existing chunks on workers, modifying on the main thread, then
encoding on workers.

```ts
// Write a full array
await setWorker(arr, null, { data, shape, stride }, { pool })

// Scalar fill
await setWorker(arr, null, 42.0, { pool })

// Partial update
await setWorker(arr, [zarr.slice(2, 8)], newData, { pool })
```

**Options** (`SetWorkerOptions`):

| Option | Type | Default | Description |
|---|---|---|---|
| `pool` | `WorkerPool` | **required** | Worker pool for codec operations |
| `workerUrl` | `string \| URL` | built-in | URL of the codec worker script |
| `useSharedArrayBuffer` | `boolean` | `false` | Use SharedArrayBuffer for intermediate buffers during partial updates |
| `signal` | `AbortSignal` | — | Aborts the write: drops queued chunk tasks, cancels reads of chunks being partially updated |

Like zarrita's `set`, a write is not transactional: chunks written before the
signal fires stay written.

### Selections

`selection` is the same positional list `zarr.get` takes — `null`, integers,
and `zarr.slice(...)` (whose bounds may be `bigint`s). zarrita's
`zarr.select` builds one from dimension names:

```ts
// arr.dimensionNames -> ["time", "y", "x"]
const frame = await getWorker(
  arr,
  zarr.select(arr, { time: 0, y: zarr.slice(0, 256) }),
  { pool },
)
```

Scalar arrays (`shape: []`) read and write like any other.

### Supported arrays

Zarr v3 arrays, and v2 arrays via the same conversion zarrita applies when it
opens them: byte order from the `dtype`, `order: "F"` as a transpose, numcodecs
filters and compressors under zarrita's `numcodecs.` codec names, and
`fixedscaleoffset` as `scale_offset` + `cast_value`. Numeric data types only
(`float16` where the runtime has `Float16Array`).

**Sharded arrays** (`sharding_indexed`) read like any other: the shard index
is range-fetched once per shard and remembered for the store's lifetime, each
inner chunk is one range request, and workers decode inner chunks with the
inner codecs. `arr.chunks` is the inner chunk shape, so selections, the
chunk cache and `useSharedArrayBuffer` all work per inner chunk. As with
zarrita, the store needs `getRange` (`FetchStore` and `FileSystemStore`
have it; `zarr.open` refuses a sharded array on a store without), and
`setWorker` rejects sharded arrays with an `UnsupportedError`, as `zarr.set`
does. Wrapping the store in `zarr.withRangeCoalescing` batches the inner
chunk requests of one shard into fewer HTTP round-trips.

Stores wrapped with zarrita's store extensions (`zarr.withByteCaching`,
`zarr.withRangeCoalescing`, `zarr.withConsolidatedMetadata`, or your own
`zarr.defineStoreExtension`) work unchanged: chunk bytes are fetched through
`arr.store` on the main thread. Array extensions (`zarr.defineArrayExtension`),
which replace `arr.getChunk`, are bypassed — fizarrita fetches and decodes
chunks itself.

## Errors

Failures are zarrita's structured errors, so the same checks work for
`getWorker`/`setWorker` as for `zarr.get`/`zarr.set`:

```ts
try {
  await getWorker(arr, selection, { pool })
} catch (e) {
  if (zarr.isZarritaError(e, 'CodecPipelineError')) {
    // e.codec, e.direction, e.chunkPath — and e.cause, the codec's own error
  } else if (zarr.isZarritaError(e, 'UnknownCodecError')) {
    // e.codec is not registered in the worker's zarrita registry
  } else if (zarr.isZarritaError(e, 'InvalidSelectionError')) {
    // out of bounds, wrong rank, zero step
  }
}
```

A codec error is raised inside a worker; the worker reports it and it is
rebuilt on the calling thread, with `chunkPath` added. Its `cause` carries the
original message but not the original error object.

## SharedArrayBuffer

Both functions accept `useSharedArrayBuffer: true` for additional performance.

**`getWorker`** — The output TypedArray is backed by `SharedArrayBuffer`.
Workers decode chunks and write directly into shared memory via the
`decode_into` protocol, eliminating one ArrayBuffer transfer and one
main-thread copy per chunk. The returned chunk can be shared with other
workers without copying.

**`setWorker`** — Intermediate chunk buffers for partial updates use
`SharedArrayBuffer`, reducing transfers between the main thread and codec
workers during the decode-modify-encode cycle.

### Requirements

`SharedArrayBuffer` requires the page to be served with:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

Vite example:

```ts
export default defineConfig({
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
})
```

If the headers are missing, `useSharedArrayBuffer: true` throws with a
descriptive error.

## Cancelling reads

A viewport-driven consumer abandons reads constantly — every pan or zoom
obsoletes tiles still in flight. Pass an `AbortSignal` so an abandoned read
stops consuming resources instead of running to completion:

```ts
const controller = new AbortController()

const read = getWorker(arr, [zarr.slice(0, 256), zarr.slice(0, 256)], {
  pool,
  signal: controller.signal,
})

// The user panned away — these tiles are stale:
controller.abort()
```

The signal is passed to each chunk `store.get` call the read makes, so stores
that honour it (e.g. `FetchStore`, whose options are a `RequestInit`) cancel
their network requests when it fires. Chunk tasks still queued on the pool are
dropped rather than started, and the returned promise rejects with the
signal's reason. A decode already running on a worker is not interrupted —
its result is discarded. The shared metadata read and chunk-shape probe are
the one exception — they run without it; see [Chunk caching](#chunk-caching).

If `opts` carries its own store-level `signal`, the two are combined: when
either fires, fetches abort, still-queued tasks are dropped, and the promise
rejects with the reason of whichever signal fired. A concurrent `getWorker`
call sharing an in-flight
chunk fetch with an aborted read is unaffected — it re-fetches the chunk
under its own signal.

## Chunk caching

`getWorker` accepts an optional `cache` to store decoded chunks. On repeated
calls the cache skips fetching and decompression entirely, returning the cached
chunk directly on the main thread.

Any object with `get(key)` and `set(key, value)` works — a plain `Map` is the
simplest option:

```ts
const cache = new Map()

// First call — fetches, decodes, and caches each chunk
const a = await getWorker(arr, null, { pool, cache })

// Second call — all chunks served from cache, no workers used
const b = await getWorker(arr, null, { pool, cache })
```

Cache keys use the format `store_N:/array/path:c/0/1/2`. A `WeakMap`-based
store ID ensures keys are unique across store instances, so a single cache can
safely be shared across multiple arrays and stores.

The array metadata read and the chunk-shape probe are memoised per
(store, array path) — both are immutable for the lifetime of an array — so
only the first `getWorker` call on an array touches the store for them. A
repeat read served entirely from a warm cache performs zero store requests.
Concurrent calls on a cold array share one resolution. Store options in `opts`
(headers, credentials, …) reach those reads too, with one exception: an
`AbortSignal` governs only the calling read's wait, never the shared
resolution — aborting one caller rejects it promptly without failing the others
that joined it, and the result still lands for the next read.

### LRU / bounded caches

For bounded memory, pass any LRU cache that implements the same `get`/`set`
interface:

```ts
import { LRUCache } from 'lru-cache'

const cache = new LRUCache({ max: 200 })
const chunk = await getWorker(arr, [zarr.slice(0, 10)], { pool, cache })
```

### When caching helps

Caching is most beneficial when:

- **Overlapping selections** — e.g. iterating over z-slices where chunks span
  many slices. Without a cache, the same chunk is decompressed for every slice
  that touches it.
- **Repeated reads** — re-reading the same region of an array (panning/zooming
  in a viewer, re-rendering a frame).
- **Large compressed chunks** — decompression dominates I/O latency, so
  avoiding it yields significant speedups.

When `useSharedArrayBuffer` is combined with `cache`, cache misses use the
standard decode path (worker returns the decoded chunk via transfer) so the
chunk can be stored in the cache. Cache hits copy the cached chunk into the
SharedArrayBuffer output on the main thread. The small overhead on first access
is repaid by subsequent cache hits that bypass workers entirely.

### `ChunkCache` interface

```ts
interface ChunkCache {
  get(key: string): Chunk<DataType> | undefined
  set(key: string, value: Chunk<DataType>): void
}
```

## Node.js

`getWorker` and `setWorker` work in plain Node with no extra setup. The package
ships two codec worker entries — `codec-worker.js` for the browser and
`codec-worker-node.js` for `node:worker_threads` — and `createDefaultWorker`
selects between them at runtime.

```ts
import { WorkerPool } from '@fideus-labs/worker-pool'
import { getWorker, setWorker } from '@fideus-labs/fizarrita'
import * as zarr from 'zarrita'

const store = new Map()
const arr = await zarr.create(zarr.root(store).resolve('/data'), {
  shape: [1024, 1024],
  chunkShape: [256, 256],
  dtype: 'int32',
})

const pool = new WorkerPool(4)
try {
  await setWorker(arr, null, 42, { pool })
  const chunk = await getWorker(arr, null, { pool })
} finally {
  // Required in Node: worker threads hold the event loop open.
  pool.terminateWorkers()
}
```

Notes:

- **Always call `pool.terminateWorkers()`** — otherwise the Node process will
  not exit.
- **`useSharedArrayBuffer` works in Node**, and needs no COOP/COEP headers there;
  `SharedArrayBuffer` is available unconditionally.
- **Codec availability is zarrita's, not ours.** zarrita ships working `zstd`,
  `blosc`, `lz4`, and `bytes` implementations; `gzip`, `zlib`, and `crc32c`
  decode but do not encode (a write fails with a `CodecPipelineError`, as it
  does in `zarr.set`) unless a codec is registered from `numcodecs`, in the
  worker, via a
  [custom codec worker](#custom-codec-worker).

## Worker message protocol

The built-in codec worker handles four message types:

| Request | Response | Description |
|---|---|---|
| `init` | `init_ok` | Register codec metadata (once per worker per array config) |
| `decode` | `decoded` | Decode raw bytes, transfer decoded ArrayBuffer back |
| `decode_into` | `decode_into_ok` | Decode and write directly into SharedArrayBuffer |
| `encode` | `encoded` | Encode chunk data (C order, or the `stride` sent with it), transfer encoded bytes back |

A failed request is answered with its usual response type carrying `error`, a
message, and — when the failure was one of zarrita's structured errors —
`errorInfo`, from which the calling thread rebuilds it.

Codec metadata is deduplicated — each unique array configuration is sent to a
worker only once, then cached by integer `metaId`.

## Custom codec worker

To use a custom worker script:

```ts
const workerUrl = new URL('./my-codec-worker.js', import.meta.url)

await getWorker(arr, null, { pool, workerUrl })
await setWorker(arr, null, data, { pool, workerUrl })
```

To build a worker yourself instead of passing a `workerUrl`, use
`createDefaultWorker()`, which returns a ready worker for the current runtime:

```ts
import { createDefaultWorker } from '@fideus-labs/fizarrita'

const worker = createDefaultWorker()
```

The two entries are also reachable as subpath exports, but they are **worker
entry modules, not importable helpers** — each one attaches a message handler to
the scope it is loaded in. Importing them on the main thread does not create a
worker: the browser entry would bind its handler to the page, and the Node entry
throws outright. Reference them as a worker script, not as an import:

```ts
// The specifier resolves to the worker file; it is never imported directly.
const url = import.meta.resolve('@fideus-labs/fizarrita/codec-worker')
await getWorker(arr, null, { pool, workerUrl: url })
```

Both are thin entry points over `handleCodecMessage`, which is exported so a
custom worker can extend the protocol without reimplementing it.

## License

[MIT](LICENSE.txt)
