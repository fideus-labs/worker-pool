/**
 * A function that receives an available worker (or null if a new worker should
 * be created) and returns a promise resolving to an object containing the
 * worker to recycle back into the pool and the task result.
 */
export type WorkerPoolTask<T> = (
  worker: Worker | null
) => Promise<{ worker: Worker; result: T }>

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
