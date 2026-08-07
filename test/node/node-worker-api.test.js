/**
 * NodeWorker / createWorker surface details: how worker paths are accepted and
 * how constructor options reach the thread.
 */
import assert from 'node:assert/strict'
import { fileURLToPath } from 'node:url'
import test from 'node:test'

import { createWorker, NodeWorker } from '../../dist/index.js'

const SQUARE_WORKER = new URL('./square-worker.mjs', import.meta.url)
const ECHO_WORKER = new URL('./echo-worker.mjs', import.meta.url)

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

test('a serialised file: URL string is accepted, not read as a path', async () => {
  // Browsers take a string URL, so `workerUrl` options are often held as one.
  // Node would otherwise treat the whole "file:///..." string as a filename.
  const worker = createWorker(SQUARE_WORKER.href)
  try {
    assert.equal(typeof SQUARE_WORKER.href, 'string')
    assert.ok(SQUARE_WORKER.href.startsWith('file://'))
    const { result } = await request(worker, { value: 6 })
    assert.equal(result, 36)
  } finally {
    worker.terminate()
  }
})

test('a plain filesystem path is passed through untouched', async () => {
  const worker = createWorker(fileURLToPath(SQUARE_WORKER))
  try {
    const { result } = await request(worker, { value: 7 })
    assert.equal(result, 49)
  } finally {
    worker.terminate()
  }
})

test('workerData reaches the thread', async () => {
  const worker = new NodeWorker(ECHO_WORKER, {
    name: 'echo',
    workerData: { hello: 'world', n: 3 },
  })
  try {
    const { workerData } = await request(worker, 'ping')
    assert.deepEqual(workerData, { hello: 'world', n: 3 })
  } finally {
    worker.terminate()
  }
})

test('terminate is idempotent and safe before the thread exists', { timeout: 10_000 }, async () => {
  const worker = createWorker(SQUARE_WORKER)
  worker.terminate()
  worker.terminate()
  // Give the deferred terminate a chance to run against a thread that may only
  // just have come into existence, then confirm the worker is really down.
  await new Promise((resolve) => setTimeout(resolve, 100))
  await assert.rejects(request(worker, { value: 1 }), /terminated/)
})

test('terminating a live worker rejects the request already in flight', { timeout: 10_000 }, async () => {
  const worker = createWorker(SQUARE_WORKER)

  // Let the thread genuinely start, so the message reaches a running worker
  // rather than the pre-startup queue.
  await request(worker, { value: 2 })

  // Now catch a request in flight. The wait matters: postMessage hands off on a
  // microtask, so terminating in the same tick is caught by the queued-message
  // guard instead and this would pass without exercising anything. Once the
  // message is genuinely on the running thread, the reply dies with it and the
  // 'exit' handler stays quiet because this exit was requested — so the
  // rejection has to come from terminate() itself or the caller waits forever.
  const pending = request(worker, { value: 3, delay: 5_000 })
  await new Promise((resolve) => setTimeout(resolve, 100))
  worker.terminate()
  await assert.rejects(pending, /terminated/)
})

test('terminating between post and thread startup still rejects the message', { timeout: 10_000 }, async () => {
  const worker = createWorker(SQUARE_WORKER)
  // The message is queued against a thread that does not exist yet, then the
  // worker is torn down before it does. Dropping it here would hang the caller.
  const pending = request(worker, { value: 5 })
  worker.terminate()
  await assert.rejects(pending, /terminated/)
})

test('removeEventListener stops delivery', async () => {
  const worker = createWorker(SQUARE_WORKER)
  try {
    const seen = []
    const listener = (event) => seen.push(event.data.result)
    worker.addEventListener('message', listener)

    await request(worker, { value: 2 })
    assert.deepEqual(seen, [4])

    worker.removeEventListener('message', listener)
    await request(worker, { value: 3 })
    assert.deepEqual(seen, [4], 'no further messages after removal')
  } finally {
    worker.terminate()
  }
})

test('an unknown event type is ignored rather than throwing', () => {
  const worker = createWorker(SQUARE_WORKER)
  try {
    const listener = () => {}
    worker.addEventListener('messageerror', listener)
    worker.removeEventListener('messageerror', listener)
  } finally {
    worker.terminate()
  }
})
