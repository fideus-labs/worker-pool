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
