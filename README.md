# @fideus-labs/worker-pool

A Web Worker pool with bounded concurrency. Provides a
[ChunkQueue](https://github.com/manzt/zarrita.js)-compatible `add` / `onIdle`
interface, backed by the scheduling engine from
[itk-wasm](https://github.com/InsightSoftwareConsortium/ITK-Wasm)'s
`WebWorkerPool`.

## Features

- **Bounded concurrency** — at most `poolSize` workers run simultaneously.
- **Worker recycling** — workers are reused (LIFO) across tasks instead of
  being re-created.
- **ChunkQueue interface** — `add()` + `onIdle()`, compatible with zarrita.js
  and p-queue patterns.
- **Batch interface** — `runTasks()` with progress reporting and cancellation.
- **Zero runtime dependencies.**

## Installation

```sh
pnpm add @fideus-labs/worker-pool
```

## Usage

### Task function contract

Every task function receives an available `Worker` (or `null` when the pool
needs a new worker created) and **must** return an object with the worker to
recycle and the result:

```ts
type WorkerPoolTask<T> = (
  worker: Worker | null
) => Promise<{ worker: Worker; result: T }>
```

### ChunkQueue interface (`add` / `onIdle`)

```ts
import { WorkerPool } from '@fideus-labs/worker-pool'

const workerUrl = new URL('./my-worker.js', import.meta.url).href

function createTask(input: number) {
  return (worker: Worker | null) => {
    const w = worker ?? new Worker(workerUrl, { type: 'module' })
    return new Promise<{ worker: Worker; result: number }>((resolve) => {
      w.onmessage = (e) => resolve({ worker: w, result: e.data })
      w.postMessage(input)
    })
  }
}

const pool = new WorkerPool(4) // 4 concurrent workers

pool.add(createTask(1))
pool.add(createTask(2))
pool.add(createTask(3))

const results = await pool.onIdle<number>()
// results: [result1, result2, result3] — in add() order

pool.terminateWorkers()
```

### zarrita.js integration

The pool satisfies the `ChunkQueue` interface, so it can be used directly with
zarrita's `create_queue` option:

```ts
import { get } from 'zarrita'
import { WorkerPool } from '@fideus-labs/worker-pool'

const pool = new WorkerPool(4)

const result = await get(arr, null, {
  create_queue: () => pool,
})
```

### Batch interface (`runTasks`)

Submit an array of tasks at once with optional progress reporting and
cancellation:

```ts
const pool = new WorkerPool(2)

const tasks = inputs.map((input) => createTask(input))

const { promise, runId } = pool.runTasks(tasks, (completed, total) => {
  console.log(`${completed}/${total}`)
})

// Cancel if needed:
// pool.cancel(runId)

const results = await promise
pool.terminateWorkers()
```

## API

### `new WorkerPool(poolSize: number)`

Create a pool with at most `poolSize` concurrent workers.

### `pool.add<T>(fn: WorkerPoolTask<T>): void`

Enqueue a task. Tasks are started when `onIdle()` is called.

### `pool.onIdle<T>(): Promise<T[]>`

Execute all enqueued tasks and wait for completion. Returns results in the
order tasks were added.

### `pool.runTasks<T>(taskFns, progressCallback?): { promise, runId }`

Submit a batch of tasks. The `promise` resolves with ordered results. The
optional `progressCallback` is invoked as
`(completedTasks: number, totalTasks: number) => void` after each task
completes.

### `pool.cancel(runId: number): void`

Cancel a pending `runTasks` batch. The promise rejects with
`'Remaining tasks canceled'`.

### `pool.terminateWorkers(): void`

Terminate all idle workers. The pool can still be used after this — new
workers will be created as needed.

## Development

```sh
pnpm install
pnpm build          # compile TypeScript
pnpm test           # run Playwright browser tests
pnpm test:ui        # interactive Playwright UI
```

## License

[MIT](LICENSE.txt)
