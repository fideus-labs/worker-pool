/**
 * The event delivered to a `'message'` listener. Only `data` is used, which is
 * all a `MessageEvent` (browser) and a synthesised Node event have in common.
 */
export interface WorkerMessageEventLike<T = any> {
  data: T
}

/**
 * The event delivered to an `'error'` listener. Browsers pass an `ErrorEvent`;
 * `node:worker_threads` passes an `Error`. `message` is the common field.
 */
export interface WorkerErrorEventLike {
  message: string
  /** The underlying error, when the runtime provides one. */
  error?: unknown
}

/**
 * The slice of the `Worker` API this pool and its tasks actually use.
 *
 * A browser `Worker` satisfies this structurally, and so does `NodeWorker`, the
 * `node:worker_threads` adapter. Typing against the interface rather than the
 * DOM global is what lets pooled code run unchanged in plain Node, where there
 * is no global `Worker`.
 */
export interface WorkerLike {
  postMessage(message: any, transfer?: any[]): void
  terminate(): void
  addEventListener(type: string, listener: (event: any) => void): void
  removeEventListener(type: string, listener: (event: any) => void): void
}

/**
 * A function that receives an available worker (or null if a new worker should
 * be created) and returns a promise resolving to an object containing the
 * worker to recycle back into the pool and the task result.
 */
export type WorkerPoolTask<T> = (
  worker: WorkerLike | null
) => Promise<{ worker: WorkerLike; result: T }>

/**
 * Progress callback invoked after each task completes.
 */
export type WorkerPoolProgressCallback = (
  completedTasks: number,
  totalTasks: number
) => void

/**
 * Return type of {@link WorkerPool.runTasks}.
 */
export interface WorkerPoolRunTasksResult<T> {
  /** Resolves with an array of results in the same order as the input tasks. */
  promise: Promise<T[]>
  /** Identifier that can be passed to {@link WorkerPool.cancel}. */
  runId: number
}

/**
 * Internal bookkeeping for a single `runTasks` invocation.
 * @internal
 */
export interface RunInfo<T> {
  taskQueue: Array<[resultIndex: number, task: WorkerPoolTask<T>]>
  results: T[]
  addingTasks: boolean
  postponed: boolean
  runningWorkers: number
  index: number
  completedTasks: number
  progressCallback: WorkerPoolProgressCallback | null
  canceled: boolean | null
  resolve?: (results: T[]) => void
  reject?: (error: unknown) => void
}
