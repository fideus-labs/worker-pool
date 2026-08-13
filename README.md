# @fideus-labs/worker-pool

[![CI](https://github.com/fideus-labs/worker-pool/actions/workflows/ci.yml/badge.svg)](https://github.com/fideus-labs/worker-pool/actions/workflows/ci.yml)

A worker pool with bounded concurrency, plus a companion
[@fideus-labs/fizarrita](#zarritajs-integration) package that accelerates
zarrita codec operations on workers. Runs on Web Workers in the browser and on
`node:worker_threads` in Node, behind one interface.

[![Watch the presentation](https://img.youtube.com/vi/D5NnpXxyJa8/maxresdefault.jpg)](https://www.youtube.com/watch?v=D5NnpXxyJa8)

## Features

- **Bounded concurrency** — at most `poolSize` workers run simultaneously.
- **Worker recycling** — workers are reused (LIFO) across tasks instead of
  being re-created.
- **ChunkQueue interface** — `add()` + `onIdle()`, compatible with zarrita.js
  and p-queue patterns.
- **Batch interface** — `runTasks()` with progress reporting and cancellation.
- **Browser and Node** — the same task code runs on Web Workers and on
  `node:worker_threads`; see [Node.js](#nodejs).
- **Zero runtime dependencies.**

## Installation

```sh
npm add @fideus-labs/worker-pool
```

## Usage

### Task function contract

Every task function receives an available worker (or `null` when the pool needs
a new worker created) and **must** return an object with the worker to recycle
and the result:

```typescript
type WorkerPoolTask<T> = (
  worker: WorkerLike | null
) => Promise<{ worker: WorkerLike; result: T }>
```

`WorkerLike` is the slice of the `Worker` API the pool and its tasks use —
`postMessage`, `terminate`, `addEventListener`, `removeEventListener`. A browser
`Worker` satisfies it, and so does the `node:worker_threads` adapter, which is
what lets one task function serve both runtimes.

### ChunkQueue interface (`add` / `onIdle`)

```typescript
import { WorkerPool } from '@fideus-labs/worker-pool'
import type { WorkerLike } from '@fideus-labs/worker-pool'

function createTask(input: number) {
  return (worker: WorkerLike | null) => {
    // Two things worth copying here:
    //  - the literal `new Worker(new URL(...), ...)` form, which is the only
    //    shape bundlers recognise as a worker entry point;
    //  - annotating the local, because `WorkerLike | Worker` is a union
    //    TypeScript resolves to the wrong `postMessage` overload.
    const w: WorkerLike =
      worker ??
      new Worker(new URL('./my-worker.js', import.meta.url), { type: 'module' })
    return new Promise<{ worker: WorkerLike; result: number }>((resolve, reject) => {
      // Handle 'error' too, or a failing worker leaves onIdle() pending forever.
      const onMessage = (e: { data: number }) => {
        detach()
        resolve({ worker: w, result: e.data })
      }
      const onError = (e: { message: string }) => {
        detach()
        reject(new Error(e.message))
      }
      const detach = () => {
        w.removeEventListener('message', onMessage)
        w.removeEventListener('error', onError)
      }
      w.addEventListener('message', onMessage)
      w.addEventListener('error', onError)
      w.postMessage(input)
    })
  }
}

const pool = new WorkerPool(4) // 4 concurrent workers

pool.add(createTask(1))
pool.add(createTask(2))
pool.add(createTask(3))

try {
  const results = await pool.onIdle<number>()
  // results: [result1, result2, result3] — in add() order
} finally {
  pool.terminateWorkers()
}
```

### Batch interface (`runTasks`)

Submit an array of tasks at once with optional progress reporting and
cancellation:

```typescript
const pool = new WorkerPool(2)

const tasks = inputs.map((input) => createTask(input))

const { promise, runId } = pool.runTasks(tasks, (completed, total) => {
  console.log(`${completed}/${total}`)
})

// Cancel if needed:
// pool.cancel(runId)

try {
  const results = await promise
} finally {
  pool.terminateWorkers()
}
```

A batch can also be tied to an `AbortSignal`. When the signal fires, tasks
that have not started are dropped and the promise rejects with the signal's
reason; tasks already running finish, but their results are discarded and
their workers return to the pool:

```typescript
const controller = new AbortController()

const { promise } = pool.runTasks(tasks, null, { signal: controller.signal })

// e.g. the viewport moved and these results are stale:
controller.abort(new Error('viewport moved'))
```

## API

### `new WorkerPool(poolSize: number)`

Create a pool with at most `poolSize` concurrent workers.

### `pool.add<T>(fn: WorkerPoolTask<T>): void`

Enqueue a task. Tasks are started when `onIdle()` is called.

### `pool.onIdle<T>(): Promise<T[]>`

Execute all enqueued tasks and wait for completion. Returns results in the
order tasks were added.

### `pool.runTasks<T>(taskFns, progressCallback?, options?): { promise, runId }`

Submit a batch of tasks. The `promise` resolves with ordered results. The
optional `progressCallback` is invoked as
`(completedTasks: number, totalTasks: number) => void` after each task
completes. `options.signal` accepts an `AbortSignal`: when it fires, tasks
that have not started are dropped and the promise rejects with the signal's
reason. A signal that is already aborted rejects the batch before any task
starts.

### `pool.cancel(runId: number): void`

Cancel a pending `runTasks` batch. The promise rejects with
`'Remaining tasks canceled'`.

### `pool.terminateWorkers(): void`

Terminate all idle workers. The pool can still be used after this — new
workers will be created as needed.

### `createWorker(url, options?): WorkerLike`

Create a worker for the current runtime: a module `Worker` where one exists
(browsers, Deno, Bun), a `NodeWorker` on Node. Throws if neither is available.

> **Bundled browser code:** bundlers only detect a worker entry point from the
> literal `new Worker(new URL('./w.js', import.meta.url), { type: 'module' })`
> form. A call through `createWorker` is opaque to them, so in a bundled browser
> app the worker script has to reach the output another way (a `?worker` import,
> a copied asset, or a literal `new Worker(...)` on the browser branch).

### `new NodeWorker(url, options?)`

A `node:worker_threads` worker behind the `WorkerLike` interface. The underlying
thread is created asynchronously — `node:worker_threads` is imported on demand so
browser bundles never try to resolve it — but the constructor is synchronous and
messages posted before the thread exists are queued in order.

`options` accepts `name` and `workerData`.

### `isNodeRuntime(): boolean`

Whether the current runtime reports itself as Node.

---

## Node.js

Plain Node has no global `Worker`, so `new Worker(...)` in a task throws
`ReferenceError: Worker is not defined`. Use `createWorker`, which picks the
right implementation for the runtime:

```typescript
import { createWorker, WorkerPool } from '@fideus-labs/worker-pool'
import type { WorkerLike } from '@fideus-labs/worker-pool'

const workerUrl = new URL('./my-worker.mjs', import.meta.url)

function createTask(input: number) {
  return async (worker: WorkerLike | null) => {
    const w = worker ?? createWorker(workerUrl)
    const result = await new Promise<number>((resolve, reject) => {
      const onMessage = (e: { data: number }) => { detach(); resolve(e.data) }
      const onError = (e: { message: string }) => { detach(); reject(new Error(e.message)) }
      const detach = () => {
        w.removeEventListener('message', onMessage)
        w.removeEventListener('error', onError)
      }
      w.addEventListener('message', onMessage)
      w.addEventListener('error', onError)
      w.postMessage(input)
    })
    return { worker: w, result }
  }
}

const pool = new WorkerPool(4)
const { promise } = pool.runTasks(inputs.map(createTask))

try {
  const results = await promise
} finally {
  // Required, and in a finally: worker threads hold the event loop open, so a
  // failed run would otherwise leave the process unable to exit.
  pool.terminateWorkers()
}
```

The worker script itself is an ordinary `node:worker_threads` module:

```javascript
import { parentPort } from 'node:worker_threads'

parentPort.on('message', (input) => parentPort.postMessage(input * input))
```

Notes:

- **Always call `pool.terminateWorkers()`.** A live worker thread keeps the Node
  event loop alive, so the process will not exit without it.
- **Event shape.** Node delivers a raw value to `on('message')` and an `Error` to
  `on('error')`; the adapter wraps them as `{ data }` and `{ message, error }` so
  listeners read the same in both runtimes.
- **`SharedArrayBuffer` and transfer lists** work as they do in the browser.

## zarrita.js Integration

The `@fideus-labs/fizarrita` package provides `getWorker` and `setWorker` as
drop-in replacements for zarrita's `get` and `set`, offloading codec
encode/decode to workers via the worker pool. It ships a codec worker for each
runtime and picks between them automatically, so the examples below work
unchanged in Node.

### Installation

```sh
pnpm add @fideus-labs/fizarrita @fideus-labs/worker-pool zarrita
```

### Basic usage

```typescript
import { WorkerPool } from '@fideus-labs/worker-pool'
import { getWorker, setWorker } from '@fideus-labs/fizarrita'
import * as zarr from 'zarrita'

const pool = new WorkerPool(4)

// Open an array
const store = new zarr.FetchStore('https://example.com/data.zarr')
const arr = await zarr.open(store, { kind: 'array' })

try {
  // Read with codec decode offloaded to workers
  const chunk = await getWorker(arr, null, { pool })

  // Write with codec encode offloaded to workers
  await setWorker(arr, null, chunk, { pool })
} finally {
  pool.terminateWorkers()
}
```

### SharedArrayBuffer support

Both `getWorker` and `setWorker` support a `useSharedArrayBuffer` option for
additional performance:

```typescript
// Read — output allocated on SharedArrayBuffer, workers decode directly
// into shared memory (eliminates one transfer + one copy per chunk)
const chunk = await getWorker(arr, null, {
  pool,
  useSharedArrayBuffer: true,
})

// chunk.data.buffer instanceof SharedArrayBuffer === true
// The chunk can be shared with other workers without copying.

// Write — intermediate buffers use SharedArrayBuffer for zero-transfer
// sharing between main thread and codec workers
await setWorker(arr, null, chunk, {
  pool,
  useSharedArrayBuffer: true,
})
```

**`getWorker` with SAB:**
- Output TypedArray is backed by `SharedArrayBuffer`
- Codec workers decode chunks AND write directly into the shared output
  buffer via the `decode_into` message protocol
- Eliminates 1 ArrayBuffer transfer (worker to main) and 1 main-thread
  `set_from_chunk` copy per chunk
- Fill-value chunks are still handled on the main thread

**`setWorker` with SAB:**
- Intermediate chunk buffers for partial updates use `SharedArrayBuffer`
- Reduces ArrayBuffer transfers between main thread and codec workers
  during the decode-modify-encode cycle

### COOP/COEP headers

`SharedArrayBuffer` requires the page to be served with these HTTP headers:

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

If these headers are missing, `useSharedArrayBuffer: true` will throw with a
descriptive error message.

**Vite example:**

```typescript
// vite.config.ts
export default defineConfig({
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
})
```

### `getWorker` options

| Option | Type | Description |
|---|---|---|
| `pool` | `WorkerPool` | **Required.** The worker pool to use. |
| `workerUrl` | `string \| URL` | URL of the codec worker script. Uses built-in default if omitted. |
| `opts` | `StoreOpts` | Pass-through options for the store's `get` method (e.g., `RequestInit`). |
| `useSharedArrayBuffer` | `boolean` | Allocate output on SharedArrayBuffer with decode-into-shared optimization. |

### `setWorker` options

| Option | Type | Description |
|---|---|---|
| `pool` | `WorkerPool` | **Required.** The worker pool to use. |
| `workerUrl` | `string \| URL` | URL of the codec worker script. Uses built-in default if omitted. |
| `useSharedArrayBuffer` | `boolean` | Use SharedArrayBuffer for intermediate chunk buffers during partial updates. |

### Worker message protocol

The codec worker handles four message types:

| Request | Response | Description |
|---|---|---|
| `init` | `init_ok` | Register codec metadata (sent once per worker per unique array config) |
| `decode` | `decoded` | Decode raw bytes, transfer decoded ArrayBuffer back |
| `decode_into` | `decode_into_ok` | Decode raw bytes and write directly into SharedArrayBuffer output |
| `encode` | `encoded` | Encode chunk data, transfer encoded bytes back |

## Benchmark

**[Live Benchmark](https://fideus-labs.github.io/worker-pool/)** — try it in
your browser.

The repository includes a benchmark app that compares vanilla zarrita `get`/`set`
with `getWorker`/`setWorker` (with and without SharedArrayBuffer):

```sh
pnpm bench
# Opens at http://localhost:5174
```

The benchmark supports both synthetic in-memory arrays and remote OME-Zarr
datasets from AWS S3. The live version is deployed to GitHub Pages on every
push to `main`.

## Development

```sh
pnpm install
pnpm dev           # Start test app dev server (port 5173)
pnpm bench         # Start benchmark app (port 5174)
pnpm test          # Run Playwright browser tests
pnpm test:ui       # Interactive Playwright UI
pnpm test:node     # Build both packages, then run the Node tests
```

## License

[MIT](LICENSE.txt)
