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
 * Options for {@link WorkerPool.runTasks}.
 */
export interface WorkerPoolRunTasksOptions {
  /**
   * Aborts the batch. When the signal fires, tasks that have not started are
   * dropped and the run's promise rejects with the signal's reason. Tasks
   * already running are not interrupted, but their results are discarded and
   * their workers are returned to the pool as they finish.
   *
   * A signal that is already aborted rejects the batch before any task starts.
   */
  signal?: AbortSignal
}

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
  /**
   * Set once the run has settled and its bookkeeping has been torn down.
   *
   * The run's entry stays in `runInfo` — indices are run IDs — so there is no
   * null to test for. A task still in flight when the run ends checks this
   * before touching any of it.
   */
  cleared: boolean
  /**
   * Detaches the run's `'abort'` listener from the caller's signal. Set only
   * when the run was given one; called by `clearTask` so a long-lived signal
   * does not keep firing into — or keep alive — a run that has settled.
   */
  abortCleanup?: () => void
  resolve?: (results: T[]) => void
  reject?: (error: unknown) => void
}
