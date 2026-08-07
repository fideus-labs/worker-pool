/**
 * WorkerPool over `node:worker_threads`, in plain Node.
 *
 * Runs against the built `dist/`, which is what npm consumers get.
 */
import assert from 'node:assert/strict'
import test from 'node:test'

import { createWorker, isNodeRuntime, NodeWorker, WorkerPool } from '../../dist/index.js'

const SQUARE_WORKER = new URL('./square-worker.mjs', import.meta.url)
const MISSING_WORKER = new URL('./no-such-worker.mjs', import.meta.url)
const EXITING_WORKER = new URL('./exiting-worker.mjs', import.meta.url)

/** Send one message and settle on the first reply or error. */
function request(worker, message) {
  return new Promise((resolve, reject) => {
    const onMessage = (event) => {
      detach()
      resolve(event.data)
    }
    const onError = (event) => {
      detach()
      reject(new Error(event.message))
    }
    const detach = () => {
      worker.removeEventListener('message', onMessage)
      worker.removeEventListener('error', onError)
    }
    worker.addEventListener('message', onMessage)
    worker.addEventListener('error', onError)
    worker.postMessage(message)
  })
}

/** A pool task in the shape the pool expects: takes a slot, returns the worker. */
function squareTask(value) {
  return async (slot) => {
    const worker = slot ?? createWorker(SQUARE_WORKER)
    const { result } = await request(worker, { value })
    return { worker, result }
  }
}

test('createWorker builds a node:worker_threads worker when there is no global Worker', () => {
  assert.equal(typeof globalThis.Worker, 'undefined')
  assert.ok(isNodeRuntime())

  const worker = createWorker(SQUARE_WORKER)
  try {
    assert.ok(worker instanceof NodeWorker)
  } finally {
    worker.terminate()
  }
})

test('runTasks schedules across node workers with bounded concurrency', async () => {
  const pool = new WorkerPool(3)
  try {
    const values = [1, 2, 3, 4, 5, 6, 7, 8]
    const { promise } = pool.runTasks(values.map(squareTask))
    assert.deepEqual(
      await promise,
      values.map((v) => v * v),
    )
    // Three slots for eight tasks — workers were reused, not created per task.
    assert.equal(pool.workerQueue.length, 3)
  } finally {
    pool.terminateWorkers()
  }
})

test('add/onIdle drains queued tasks', async () => {
  const pool = new WorkerPool(2)
  try {
    for (const value of [3, 4, 5]) pool.add(squareTask(value))
    assert.deepEqual(await pool.onIdle(), [9, 16, 25])
  } finally {
    pool.terminateWorkers()
  }
})

test('a message accepts a transfer list', async () => {
  const pool = new WorkerPool(1)
  try {
    const { promise } = pool.runTasks([
      async (slot) => {
        const worker = slot ?? createWorker(SQUARE_WORKER)
        const payload = new Uint8Array([1, 2, 3, 4]).buffer
        const { result } = await new Promise((resolve, reject) => {
          const onMessage = (event) => {
            detach()
            resolve(event.data)
          }
          const onError = (event) => {
            detach()
            reject(new Error(event.message))
          }
          const detach = () => {
            worker.removeEventListener('message', onMessage)
            worker.removeEventListener('error', onError)
          }
          worker.addEventListener('message', onMessage)
          worker.addEventListener('error', onError)
          worker.postMessage({ value: 9, payload }, [payload])
        })
        assert.equal(payload.byteLength, 0, 'buffer should be detached by transfer')
        return { worker, result }
      },
    ])
    assert.deepEqual(await promise, [81])
  } finally {
    pool.terminateWorkers()
  }
})

test('an error thrown inside the worker rejects the request', { timeout: 10_000 }, async () => {
  const worker = createWorker(SQUARE_WORKER)
  try {
    await assert.rejects(request(worker, { fail: true }), /intentional worker failure/)
  } finally {
    worker.terminate()
  }
})

test('a worker that cannot start rejects the request that follows', { timeout: 10_000 }, async () => {
  const worker = createWorker(MISSING_WORKER)
  try {
    await assert.rejects(request(worker, { value: 1 }))
  } finally {
    worker.terminate()
  }
})

test('a startup failure landing before any listener is replayed, not swallowed', { timeout: 10_000 }, async () => {
  const worker = createWorker(MISSING_WORKER)
  try {
    // Nothing is subscribed while the worker fails to load. Without the latch
    // in NodeWorker, the request posted afterwards would never settle.
    await new Promise((resolve) => setTimeout(resolve, 250))
    await assert.rejects(request(worker, { value: 1 }))
  } finally {
    worker.terminate()
  }
})

test('a worker that exits without replying rejects rather than hangs', { timeout: 10_000 }, async () => {
  const worker = createWorker(EXITING_WORKER)
  try {
    await assert.rejects(request(worker, { value: 1 }), /exit code 0/)
  } finally {
    worker.terminate()
  }
})

test('posting after terminate reports an error rather than going quiet', { timeout: 10_000 }, async () => {
  const worker = createWorker(SQUARE_WORKER)
  worker.terminate()
  await assert.rejects(request(worker, { value: 1 }), /terminated/)
})

test('messages posted before the thread exists keep their order', { timeout: 10_000 }, async () => {
  const worker = createWorker(SQUARE_WORKER)
  try {
    const seen = []
    worker.addEventListener('message', (event) => seen.push(event.data.result))

    // All four are posted synchronously, before the dynamic import of
    // node:worker_threads has even resolved.
    for (const value of [1, 2, 3, 4]) worker.postMessage({ value })

    await new Promise((resolve) => {
      const check = () => (seen.length === 4 ? resolve() : setTimeout(check, 10))
      check()
    })
    assert.deepEqual(seen, [1, 4, 9, 16])
  } finally {
    worker.terminate()
  }
})
