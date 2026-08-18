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

/** A stand-in worker that records how often it is terminated. */
function stubWorker() {
  return {
    terminated: 0,
    postMessage() {},
    terminate() { this.terminated++ },
    addEventListener() {},
    removeEventListener() {},
  }
}

/** Poll until `predicate` holds, failing the test after ~2s rather than hanging. */
async function waitFor(predicate, message) {
  for (let i = 0; i < 200; i++) {
    if (predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  assert.fail(message)
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

test('a rejected task gives its slot back to the pool', async () => {
  const pool = new WorkerPool(2)
  try {
    await assert.rejects(
      pool.runTasks([async () => { throw new Error('boom') }]).promise,
      /boom/,
    )
    assert.equal(pool.workerQueue.length, 2, 'the pool kept both slots')
  } finally {
    pool.terminateWorkers()
  }
})

// The slot has to come back on the failure path too. When it does not, each
// rejection shrinks the pool by one, and once every slot is gone the scheduler
// has nothing left to hand out: this batch would never start, and the test
// would fail on its timeout rather than on an assertion.
test('a pool that has failed on every slot still runs the next batch', { timeout: 10_000 }, async () => {
  const pool = new WorkerPool(2)
  try {
    for (let i = 0; i < 4; i++) {
      await assert.rejects(
        pool.runTasks([async () => { throw new Error(`boom ${i}`) }]).promise,
        /boom/,
      )
    }
    assert.equal(pool.workerQueue.length, 2)

    const { promise } = pool.runTasks([squareTask(6), squareTask(7)])
    assert.deepEqual(await promise, [36, 49])
  } finally {
    pool.terminateWorkers()
  }
})

test('a rejected task terminates the worker it was lent instead of leaking it', async () => {
  const pool = new WorkerPool(1)
  const lent = stubWorker()

  // Park the stub worker in the pool's only slot.
  await pool.runTasks([async () => ({ worker: lent, result: 1 })]).promise
  assert.equal(pool.workerQueue[0], lent)

  // The next task is handed that worker and fails while holding it. Nothing
  // else has a reference to it, so the pool is the only thing that can shut it
  // down — a live worker thread keeps the Node process from exiting.
  await assert.rejects(
    pool.runTasks([async () => { throw new Error('boom') }]).promise,
    /boom/,
  )

  assert.equal(lent.terminated, 1)
  assert.equal(pool.workerQueue.length, 1)
  assert.equal(pool.workerQueue[0], null, 'a failed worker is not handed to the next task')
})

// The rejection handler is chained after the fulfillment handler, so it also
// runs when that handler throws — by which point the worker has already been
// recycled. Treating that as a task failure would terminate a healthy worker
// still sitting in the queue and push a second slot for the one task.
test('a progress callback that throws does not cost the pool its worker', async () => {
  const pool = new WorkerPool(1)
  const lent = stubWorker()
  const task = async () => ({ worker: lent, result: 1 })

  await pool.runTasks([task]).promise
  assert.equal(pool.workerQueue[0], lent)

  const { promise } = pool.runTasks([task], () => {
    throw new Error('callback boom')
  })
  await assert.rejects(promise, /callback boom/)

  assert.equal(lent.terminated, 0, 'the recycled worker is still healthy')
  assert.equal(pool.workerQueue.length, 1, 'the slot came back exactly once')
  assert.equal(pool.workerQueue[0], lent, 'and it came back holding its worker')
})

// The first rejection settles the batch and tears the run down, so the second
// lands on a run whose `reject` is already a no-op. Its slot and its worker are
// still the pool's responsibility.
test('every task in a batch can fail without leaking a slot or a worker', async () => {
  const unhandled = []
  const onUnhandled = (reason) => unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)

  const pool = new WorkerPool(2)
  const lent = [stubWorker(), stubWorker()]
  try {
    // Park both stubs in the pool so the failing batch is handed real workers.
    await pool.runTasks([
      async () => ({ worker: lent[0], result: 0 }),
      async () => ({ worker: lent[1], result: 1 }),
    ]).promise
    assert.equal(pool.workerQueue.length, 2)

    await assert.rejects(
      pool.runTasks([
        async () => { throw new Error('boom 0') },
        async () => { throw new Error('boom 1') },
      ]).promise,
      /boom/,
    )

    await waitFor(() => pool.workerQueue.length === 2, 'a failed task kept its slot')
    assert.deepEqual(pool.workerQueue, [null, null])
    assert.deepEqual(
      lent.map((w) => w.terminated),
      [1, 1],
      'both lent workers were shut down, including the one whose run was already cleared',
    )

    // Let any stray rejection surface before asserting there was none.
    await new Promise((resolve) => setTimeout(resolve, 50))
    assert.deepEqual(unhandled, [])

    assert.deepEqual(await pool.runTasks([squareTask(4), squareTask(5)]).promise, [16, 25])
  } finally {
    process.off('unhandledRejection', onUnhandled)
    pool.terminateWorkers()
  }
})

test('a task that throws before returning a promise gives its slot back', async () => {
  const pool = new WorkerPool(2)
  try {
    await assert.rejects(
      pool.runTasks([() => { throw new Error('sync boom') }]).promise,
      /sync boom/,
    )
    assert.equal(pool.workerQueue.length, 2, 'the pool kept both slots')

    assert.deepEqual(await pool.runTasks([squareTask(3)]).promise, [9])
  } finally {
    pool.terminateWorkers()
  }
})

// The retry branch calls the task from a `setTimeout` callback, where a
// synchronous throw has no caller to catch it: it escapes as an uncaught
// exception — fatal to the process — and the batch promise never settles.
test('a synchronous throw from the retry branch does not escape the pool', { timeout: 10_000 }, async () => {
  const pool = new WorkerPool(1)
  const stub = stubWorker()

  // Batch 1 takes the pool's only slot and holds it.
  const first = pool.runTasks([
    async () => {
      await new Promise((resolve) => setTimeout(resolve, 150))
      return { worker: stub, result: 1 }
    },
  ]).promise

  // Batch 2 finds no free slot and nothing of its own running, so it postpones
  // and retries on a timer. A retry lands after batch 1 frees the slot, and
  // runs the throwing task from inside that timer.
  await new Promise((resolve) => setTimeout(resolve, 20))
  const second = pool.runTasks([() => { throw new Error('sync boom from timer') }]).promise

  await assert.rejects(second, /sync boom from timer/)
  assert.deepEqual(await first, [1])
  assert.equal(pool.workerQueue.length, 1, 'the retried slot came back')
})

test('a task still in flight when its batch fails gives its slot back too', async () => {
  const pool = new WorkerPool(2)
  try {
    let release
    const held = new Promise((resolve) => { release = resolve })

    const { promise } = pool.runTasks([
      async () => { throw new Error('boom') },
      async (slot) => {
        const worker = slot ?? createWorker(SQUARE_WORKER)
        await held
        const { result } = await request(worker, { value: 5 })
        return { worker, result }
      },
    ])

    await assert.rejects(promise, /boom/)
    // The batch has already rejected while the second task holds a slot.
    assert.equal(pool.workerQueue.length, 1)

    // That straggler settles into a run that is already torn down. Its slot
    // still has to come back, or the pool ends the batch a worker short.
    release()
    await waitFor(
      () => pool.workerQueue.length === 2,
      'the in-flight task never returned its slot',
    )

    assert.deepEqual(await pool.runTasks([squareTask(6), squareTask(7)]).promise, [36, 49])
  } finally {
    pool.terminateWorkers()
  }
})

// `clearTask` empties a settled run's bookkeeping but leaves its entry in
// `runInfo`, because indices are run IDs. A straggler that writes its result in
// afterwards refills what was just emptied, and nothing ever empties it again:
// a failed run never comes back to `runningWorkers === 0`, so its second
// `clearTask` never runs and the result is retained for the pool's lifetime.
test('a straggler does not refill the bookkeeping of a run that already settled', async () => {
  const pool = new WorkerPool(2)
  const settled = []
  try {
    let release
    const held = new Promise((resolve) => { release = resolve })

    const { promise, runId } = pool.runTasks([
      async () => { throw new Error('boom') },
      async () => {
        await held
        return { worker: stubWorker(), result: 'straggler' }
      },
    ])
    await assert.rejects(promise, /boom/)

    // Reach into the pool's own bookkeeping: this is about state the caller
    // cannot see, so there is nothing else to assert against.
    const info = pool.runInfo[runId]
    assert.deepEqual(info.results, [], 'the run was torn down')

    release()
    await waitFor(() => pool.workerQueue.length === 2, 'the straggler kept its slot')
    settled.push('straggler done')

    assert.deepEqual(info.results, [], 'the straggler left the cleared run alone')
    assert.equal(info.completedTasks, 0)
    assert.equal(info.progressCallback, null)
  } finally {
    assert.deepEqual(settled, ['straggler done'])
    pool.terminateWorkers()
  }
})

test('an already-aborted signal rejects the batch before any task starts', async () => {
  const pool = new WorkerPool(2)
  const controller = new AbortController()
  controller.abort(new Error('too late'))

  let started = 0
  const { promise } = pool.runTasks(
    [async () => { started++; return { worker: stubWorker(), result: 1 } }],
    null,
    { signal: controller.signal },
  )

  await assert.rejects(promise, /too late/)
  assert.equal(started, 0, 'no task should start under a fired signal')
  assert.equal(pool.workerQueue.length, 2, 'no slot was ever taken')
})

test('aborting mid-run drops queued tasks and rejects with the reason', async () => {
  const pool = new WorkerPool(1)
  const controller = new AbortController()

  let release
  const held = new Promise((resolve) => { release = resolve })
  const ran = []
  const task = (id) => async () => {
    ran.push(id)
    if (id === 0) await held
    return { worker: stubWorker(), result: id }
  }

  const { promise } = pool.runTasks(
    [task(0), task(1), task(2)],
    null,
    { signal: controller.signal },
  )
  await waitFor(() => ran.length === 1, 'the first task should start')

  controller.abort(new Error('view changed'))
  await assert.rejects(promise, /view changed/)

  // The in-flight task settles into the torn-down run; its slot still comes back.
  release()
  await waitFor(() => pool.workerQueue.length === 1, 'the straggler kept its slot')

  // Give any stray dispatch a chance to run before asserting it did not.
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.deepEqual(ran, [0], 'the queued tasks were dropped, not started')
})

test('a signal that fires after the batch settled is inert', async () => {
  const unhandled = []
  const onUnhandled = (reason) => unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)

  const pool = new WorkerPool(1)
  const controller = new AbortController()
  try {
    const { promise } = pool.runTasks(
      [async () => ({ worker: stubWorker(), result: 7 })],
      null,
      { signal: controller.signal },
    )
    assert.deepEqual(await promise, [7])

    // The listener was detached when the run settled, so this lands nowhere.
    controller.abort(new Error('after the fact'))
    await new Promise((resolve) => setTimeout(resolve, 50))
    assert.deepEqual(unhandled, [])
    assert.equal(pool.workerQueue.length, 1)
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
})

// A batch with no free slot and nothing of its own running parks its task on a
// 50ms retry timer. When the abort lands while the task is parked, the retry
// fires into a run that has already been torn down — the task must be dropped
// there, not started against a settled batch.
test('a task parked on the retry timer when the abort lands never starts', { timeout: 10_000 }, async () => {
  const pool = new WorkerPool(1)
  const stub = stubWorker()

  let release
  const held = new Promise((resolve) => { release = resolve })
  const first = pool.runTasks([
    async () => { await held; return { worker: stub, result: 1 } },
  ]).promise

  // No slot free, nothing of batch 2's running: its task postpones.
  await new Promise((resolve) => setTimeout(resolve, 20))
  const controller = new AbortController()
  let ran = false
  const second = pool.runTasks(
    [async () => { ran = true; return { worker: stubWorker(), result: 2 } }],
    null,
    { signal: controller.signal },
  ).promise

  controller.abort(new Error('gone'))
  await assert.rejects(second, /gone/)

  release()
  assert.deepEqual(await first, [1])

  // Let the 50ms retry fire into the cleared run before asserting.
  await new Promise((resolve) => setTimeout(resolve, 120))
  assert.equal(ran, false, 'the parked task never started')
  assert.equal(pool.workerQueue.length, 1, 'the pool kept its slot')
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
