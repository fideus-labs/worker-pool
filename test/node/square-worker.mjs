/**
 * Minimal `node:worker_threads` worker used by the Node pool tests.
 *
 * Squares `value`, or throws when asked to, so the adapter's error path has
 * something to report.
 */
import { parentPort } from 'node:worker_threads'

parentPort.on('message', ({ value, fail }) => {
  if (fail) throw new Error('intentional worker failure')
  parentPort.postMessage({ result: value * value })
})
