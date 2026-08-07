/**
 * Reports back what the worker was constructed with, so `NodeWorkerOptions`
 * pass-through can be checked from the main thread.
 */
import { parentPort, workerData } from 'node:worker_threads'

parentPort.on('message', () => {
  parentPort.postMessage({ workerData })
})
