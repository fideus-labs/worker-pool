/**
 * A worker that exits cleanly instead of replying — the case where a request
 * would otherwise be left pending forever.
 */
import { parentPort } from 'node:worker_threads'

parentPort.on('message', () => {
  process.exit(0)
})
