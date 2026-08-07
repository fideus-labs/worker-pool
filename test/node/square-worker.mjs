/**
 * Minimal `node:worker_threads` worker used by the Node pool tests.
 *
 * Squares `value`, optionally after `delay` ms so a request can be caught in
 * flight, or throws when asked to, so the adapter's error path has something to
 * report.
 */
import { parentPort } from 'node:worker_threads'

parentPort.on('message', ({ value, fail, delay }) => {
  if (fail) throw new Error('intentional worker failure')
  if (delay) {
    setTimeout(() => parentPort.postMessage({ result: value * value }), delay)
    return
  }
  parentPort.postMessage({ result: value * value })
})
