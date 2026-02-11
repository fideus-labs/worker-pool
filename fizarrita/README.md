# @fideus-labs/fizarrita

Worker-pool-accelerated `get`/`set` for [zarrita.js](https://github.com/manzt/zarrita.js) — offloads codec encode/decode to Web Workers via [@fideus-labs/worker-pool](https://github.com/fideus-labs/worker-pool).

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

// Read — codec decode runs on workers
const chunk = await getWorker(arr, null, { pool })

// Write — codec encode runs on workers
await setWorker(arr, null, chunk, { pool })

pool.terminateWorkers()
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

## Worker message protocol

The built-in codec worker handles four message types:

| Request | Response | Description |
|---|---|---|
| `init` | `init_ok` | Register codec metadata (once per worker per array config) |
| `decode` | `decoded` | Decode raw bytes, transfer decoded ArrayBuffer back |
| `decode_into` | `decode_into_ok` | Decode and write directly into SharedArrayBuffer |
| `encode` | `encoded` | Encode chunk data, transfer encoded bytes back |

Codec metadata is deduplicated — each unique array configuration is sent to a
worker only once, then cached by integer `metaId`.

## Custom codec worker

To use a custom worker script:

```ts
const workerUrl = new URL('./my-codec-worker.js', import.meta.url)

await getWorker(arr, null, { pool, workerUrl })
await setWorker(arr, null, data, { pool, workerUrl })
```

The built-in worker is also available as a subpath export for direct reference:

```ts
import '@fideus-labs/fizarrita/codec-worker'
```

## License

[MIT](LICENSE.txt)
