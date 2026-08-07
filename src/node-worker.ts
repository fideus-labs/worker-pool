/**
 * `node:worker_threads` support.
 *
 * Plain Node has no global `Worker`, so every pooled task that falls back to
 * `new Worker(...)` throws there. {@link NodeWorker} wraps a worker thread in
 * the small slice of the browser `Worker` surface the pool and its tasks use
 * ({@link WorkerLike}), so the same task code runs unchanged in both runtimes.
 *
 * `node:worker_threads` is imported dynamically through a variable specifier.
 * This module ships in browser builds too — nothing calls it there — and a
 * literal `import('node:worker_threads')` would make Vite, Rollup, and webpack
 * try to resolve a module that does not exist for the browser.
 */

import type {
  WorkerErrorEventLike,
  WorkerLike,
  WorkerMessageEventLike,
} from './types.js'

// ---------------------------------------------------------------------------
// node:worker_threads, loaded on demand
// ---------------------------------------------------------------------------

/** The part of `node:worker_threads`'s `Worker` that {@link NodeWorker} drives. */
interface NodeWorkerThread {
  on(event: 'message', listener: (value: unknown) => void): void
  on(event: 'error', listener: (err: Error) => void): void
  on(event: 'exit', listener: (exitCode: number) => void): void
  postMessage(value: unknown, transferList?: unknown[]): void
  terminate(): Promise<number>
}

type NodeWorkerThreadCtor = new (
  filename: string | URL,
  options?: Record<string, unknown>
) => NodeWorkerThread

let threadCtor: Promise<NodeWorkerThreadCtor> | null = null

function loadNodeWorkerCtor(): Promise<NodeWorkerThreadCtor> {
  if (threadCtor === null) {
    const specifier = 'node:worker_threads'
    threadCtor = import(
      /* @vite-ignore */ /* webpackIgnore: true */ specifier
    ).then((mod: { Worker: NodeWorkerThreadCtor }) => mod.Worker)
  }
  return threadCtor
}

/** Whether the current runtime is Node (or a runtime reporting itself as Node). */
export function isNodeRuntime(): boolean {
  const proc = (globalThis as { process?: { versions?: { node?: string } } })
    .process
  return typeof proc?.versions?.node === 'string'
}

/**
 * Node reads a bare string as a filesystem path, so a serialised URL — which is
 * what browsers accept, and what `workerUrl`-style options usually hold — has to
 * be turned back into a `URL` first.
 *
 * The scheme must be at least two characters so a Windows path like `C:\w.js`
 * is left alone.
 */
function normalizeWorkerPath(url: string | URL): string | URL {
  if (typeof url !== 'string') return url
  return /^[a-z][a-z0-9+.-]+:/i.test(url) ? new URL(url) : url
}

// ---------------------------------------------------------------------------
// NodeWorker
// ---------------------------------------------------------------------------

export interface NodeWorkerOptions {
  /** Worker name, surfaced by the Node inspector. */
  name?: string
  /** Value exposed to the worker as `workerData`. */
  workerData?: unknown
}

/**
 * A `node:worker_threads` worker behind the browser `Worker` interface.
 *
 * The underlying thread is created asynchronously (the module import above is
 * dynamic), but the constructor is synchronous so it can be a drop-in for
 * `new Worker(...)`. Messages posted before the thread exists are queued on the
 * same promise chain, which preserves their order.
 *
 * The worker keeps the Node event loop alive until it is terminated — call
 * `WorkerPool.terminateWorkers()` (or {@link NodeWorker.terminate}) when done,
 * or the process will not exit.
 */
export class NodeWorker implements WorkerLike {
  private thread: Promise<NodeWorkerThread>
  private messageListeners = new Set<(event: WorkerMessageEventLike) => void>()
  private errorListeners = new Set<(event: WorkerErrorEventLike) => void>()
  private terminated = false

  /**
   * The first error seen, latched.
   *
   * A failure can land before anything has subscribed — the thread import may
   * reject before the caller attaches its listeners — so the error is kept and
   * replayed on `postMessage` and on late `'error'` subscriptions. Without the
   * replay a request posted to a dead worker would simply never settle.
   */
  private fatal: WorkerErrorEventLike | null = null

  constructor(url: string | URL, options: NodeWorkerOptions = {}) {
    const filename = normalizeWorkerPath(url)
    this.thread = loadNodeWorkerCtor().then((Ctor) => {
      const thread = new Ctor(filename, options as Record<string, unknown>)
      thread.on('message', (value) => this.dispatchMessage({ data: value }))
      thread.on('error', (err) => this.emitError(err))
      thread.on('exit', (code) => {
        // Any unrequested exit, clean or not: the thread is gone, so anything
        // still waiting on a reply would otherwise wait forever.
        if (!this.terminated) {
          this.emitError(new Error(`Worker stopped with exit code ${code}`))
        }
      })
      return thread
    })
    this.thread.catch((err: unknown) => this.emitError(err))
  }

  /**
   * Unlike a browser `Worker`, posting to a dead worker raises an `'error'`
   * event rather than doing nothing. Silence is worse here: the callers are RPC
   * layers waiting on a reply, and a dropped message reads to them as a request
   * that never finishes.
   */
  postMessage(message: any, transfer?: any[]): void {
    if (this.terminated) {
      const event: WorkerErrorEventLike = {
        message: 'Worker has been terminated',
      }
      queueMicrotask(() => this.dispatchError(event))
      return
    }

    if (this.fatal !== null) {
      // Already dead — re-raise so the request this message belongs to is
      // rejected rather than left pending forever.
      const event = this.fatal
      queueMicrotask(() => this.dispatchError(event))
      return
    }

    this.thread.then(
      (thread) => {
        if (this.terminated) {
          // Terminated while this message sat waiting for the thread. Report it
          // for the same reason as above, rather than dropping it on the floor.
          this.dispatchError({ message: 'Worker has been terminated' })
          return
        }
        try {
          thread.postMessage(message, transfer)
        } catch (err) {
          this.emitError(err)
        }
      },
      (err: unknown) => this.emitError(err)
    )
  }

  terminate(): void {
    this.terminated = true
    this.thread.then(
      (thread) => {
        thread.terminate().catch(() => {})
      },
      () => {}
    )
  }

  addEventListener(type: string, listener: (event: any) => void): void {
    if (type === 'message') {
      this.messageListeners.add(listener)
      return
    }
    if (type === 'error') {
      this.errorListeners.add(listener)
      if (this.fatal !== null) {
        const event = this.fatal
        queueMicrotask(() => listener(event))
      }
    }
  }

  removeEventListener(type: string, listener: (event: any) => void): void {
    if (type === 'message') this.messageListeners.delete(listener)
    else if (type === 'error') this.errorListeners.delete(listener)
  }

  private dispatchMessage(event: WorkerMessageEventLike): void {
    for (const listener of this.messageListeners) listener(event)
  }

  private dispatchError(event: WorkerErrorEventLike): void {
    for (const listener of this.errorListeners) listener(event)
  }

  private emitError(err: unknown): void {
    const event: WorkerErrorEventLike = {
      message: err instanceof Error ? err.message : String(err),
      error: err,
    }
    this.fatal ??= event
    this.dispatchError(event)
  }
}

// ---------------------------------------------------------------------------
// createWorker
// ---------------------------------------------------------------------------

/**
 * Create a worker for the current runtime: a module `Worker` where one exists
 * (browsers, Deno, Bun), a {@link NodeWorker} on Node.
 *
 * Note for bundled browser code: bundlers only detect a worker entry point from
 * the literal `new Worker(new URL('./w.js', import.meta.url), { type: 'module' })`
 * form. A call through this function is opaque to them, so the worker script has
 * to reach the output some other way (a `?worker` import, a copied asset, or a
 * literal `new Worker(...)` on the browser branch — which is what
 * `fizarrita`'s `createDefaultWorker` does).
 *
 * @param url     - Worker script URL. Node also accepts a `./`-relative path.
 * @param options - Worker `name`, plus `workerData`, which has no browser
 *   equivalent and is ignored when a global `Worker` is used.
 */
export function createWorker(
  url: string | URL,
  options: NodeWorkerOptions = {}
): WorkerLike {
  const WorkerCtor = (
    globalThis as {
      Worker?: new (url: string | URL, options?: unknown) => WorkerLike
    }
  ).Worker

  if (typeof WorkerCtor === 'function') {
    return new WorkerCtor(url, { type: 'module', name: options.name })
  }
  if (isNodeRuntime()) {
    return new NodeWorker(url, options)
  }
  throw new Error(
    'No Worker implementation available: this runtime has no global `Worker` ' +
      'and is not Node.'
  )
}
