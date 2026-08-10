import type {
  WorkerPoolTask,
  WorkerPoolProgressCallback,
  WorkerPoolRunTasksResult,
  WorkerLike,
  RunInfo,
} from './types.js'

/**
 * A pool of workers that schedules tasks with bounded concurrency.
 *
 * Worker slots are typed as {@link WorkerLike}, so the same pool drives browser
 * `Worker`s and `node:worker_threads` workers (see `createWorker`).
 *
 * Provides two usage patterns:
 *
 * 1. **ChunkQueue interface** — `add()` to enqueue individual tasks, then
 *    `await onIdle()` to wait for all of them to complete.
 *
 * 2. **Batch interface** — `runTasks()` to submit an array of tasks at once,
 *    with optional progress reporting and cancellation support.
 *
 * Ported from the itk-wasm `WebWorkerPool` implementation.
 */
class WorkerPool {
  /** Available (idle) workers. Uses LIFO (push/pop) for warm reuse. */
  workerQueue: Array<WorkerLike | null>

  /** Bookkeeping for each `runTasks` / `onIdle` invocation. */
  private runInfo: Array<RunInfo<unknown>>

  /**
   * Accumulated tasks from `add()` calls, drained by the next `onIdle()`.
   * @internal
   */
  private pendingTasks: Array<WorkerPoolTask<unknown>>

  /**
   * @param poolSize - Maximum number of concurrent web workers.
   */
  constructor(poolSize: number) {
    this.workerQueue = new Array<WorkerLike | null>(poolSize)
    this.workerQueue.fill(null)
    this.runInfo = []
    this.pendingTasks = []
  }

  // ---------------------------------------------------------------------------
  // ChunkQueue-compatible interface
  // ---------------------------------------------------------------------------

  /**
   * Enqueue a single task for execution.
   *
   * The provided function receives an available worker (or `null` when a new
   * worker should be created) and must return `{ worker, result }` so the pool
   * can recycle the worker.
   *
   * Tasks are not started until {@link onIdle} is called.
   *
   * @param fn - Task function.
   */
  add<T>(fn: WorkerPoolTask<T>): void {
    this.pendingTasks.push(fn as WorkerPoolTask<unknown>)
  }

  /**
   * Execute all tasks previously enqueued via {@link add} and wait for them
   * to complete.
   *
   * @returns An array of results in the same order tasks were added.
   */
  async onIdle<T>(): Promise<T[]> {
    const tasks = this.pendingTasks.splice(0)
    if (tasks.length === 0) {
      return []
    }
    const { promise } = this.runTasks<unknown>(tasks as Array<WorkerPoolTask<unknown>>)
    return promise as Promise<T[]>
  }

  // ---------------------------------------------------------------------------
  // Batch interface (itk-wasm style)
  // ---------------------------------------------------------------------------

  /**
   * Submit an array of tasks for execution.
   *
   * @param taskFns          - Array of task functions.
   * @param progressCallback - Optional callback invoked after each task
   *                           completes.
   * @returns An object with a `promise` that resolves with ordered results and
   *          a `runId` for cancellation.
   */
  runTasks<T>(
    taskFns: Array<WorkerPoolTask<T>>,
    progressCallback: WorkerPoolProgressCallback | null = null
  ): WorkerPoolRunTasksResult<T> {
    const info: RunInfo<T> = {
      taskQueue: [],
      results: [],
      addingTasks: false,
      postponed: false,
      runningWorkers: 0,
      index: 0,
      completedTasks: 0,
      progressCallback,
      canceled: false,
      cleared: false,
    }
    this.runInfo.push(info as RunInfo<unknown>)
    info.index = this.runInfo.length - 1

    return {
      promise: new Promise<T[]>((resolve, reject) => {
        info.resolve = resolve
        info.reject = reject

        info.results = new Array<T>(taskFns.length)
        info.completedTasks = 0

        info.addingTasks = true
        taskFns.forEach((fn, index) => {
          this.addTask<T>(info.index, index, fn)
        })
        info.addingTasks = false
      }),
      runId: info.index,
    }
  }

  /**
   * Terminate all idle workers in the pool. Workers currently executing tasks
   * are not affected — they will be replaced with `null` slots once they
   * complete.
   */
  terminateWorkers(): void {
    for (let i = 0; i < this.workerQueue.length; i++) {
      const worker = this.workerQueue[i]
      if (worker != null) {
        worker.terminate()
      }
      this.workerQueue[i] = null
    }
  }

  /**
   * Cancel a pending `runTasks` batch. The returned promise will reject with
   * `'Remaining tasks canceled'`.
   *
   * @param runId - The `runId` returned by {@link runTasks}.
   */
  cancel(runId: number): void {
    const info = this.runInfo[runId]
    if (info != null) {
      info.canceled = true
    }
  }

  // ---------------------------------------------------------------------------
  // Internal scheduling — ported from itk-wasm
  // ---------------------------------------------------------------------------

  /**
   * Core scheduler. Three branches:
   *
   * 1. **Worker available** — pop it, run the task, return the slot once the
   *    task settles (recycled on success, emptied on failure), then
   *    chain-schedule the next queued task.
   * 2. **No worker, but work in progress** — push onto the overflow queue;
   *    a completing worker will pick it up.
   * 3. **No worker, nothing in progress** — retry after a short delay (handles
   *    a race when multiple concurrent `runTasks` calls compete for workers).
   *
   * @internal
   */
  private addTask<T>(
    infoIndex: number,
    resultIndex: number,
    task: WorkerPoolTask<T>
  ): void {
    const info = this.runInfo[infoIndex] as RunInfo<T> | undefined

    if (info?.canceled === true) {
      info.reject!('Remaining tasks canceled')
      this.clearTask(info.index)
      return
    }

    if (this.workerQueue.length > 0) {
      const worker = this.workerQueue.pop() as WorkerLike | null
      info!.runningWorkers++

      // A popped slot has to come back exactly once, however the task settles.
      // Returning it only on success shrinks the pool for good: after
      // `poolSize` failures the queue is empty, and from there every `addTask`
      // takes the retry branch below and reschedules itself every 50ms with no
      // slot left to claim — the next batch never runs.
      let slotReturned = false
      const returnSlot = (recycled: WorkerLike | null): void => {
        if (slotReturned) return
        slotReturned = true
        this.workerQueue.push(recycled)
      }

      // A task that throws before returning its promise never reaches the
      // handlers below, so the slot it was lent would stay popped. Worse, the
      // retry branch calls `addTask` from a timer, where the throw has no
      // caller to catch it: it escapes as an uncaught exception and the batch
      // never settles. Normalised into a rejection, it takes the same path as
      // a task that rejects.
      let running: Promise<{ worker: WorkerLike; result: T }>
      try {
        running = Promise.resolve(task(worker))
      } catch (error) {
        running = Promise.reject(error)
      }

      running
        .then(({ worker: returnedWorker, result }) => {
          returnSlot(returnedWorker)

          // Guard: the run may have been cleared while this task was in-flight.
          // The slot above still has to come back — that is the pool's, not the
          // run's — but everything below belongs to a run that has already
          // settled, and writing to it would refill the bookkeeping `clearTask`
          // just emptied. That memory is never reclaimed: `runInfo` entries are
          // kept forever because their indices are run IDs, and a failed run
          // never returns to `runningWorkers === 0` to be cleared a second time.
          if (!info!.cleared) {
            info!.runningWorkers--
            info!.results[resultIndex] = result
            info!.completedTasks++

            if (info!.progressCallback != null) {
              info!.progressCallback(info!.completedTasks, info!.results.length)
            }

            if (info!.taskQueue.length > 0) {
              const [nextResultIndex, nextTask] = info!.taskQueue.shift()!
              this.addTask<T>(infoIndex, nextResultIndex, nextTask as WorkerPoolTask<T>)
            } else if (!info!.addingTasks && info!.runningWorkers === 0) {
              const results = info!.results
              info!.resolve!(results)
              this.clearTask(info!.index)
            }
          }
        })
        .catch((error: unknown) => {
          // Two ways to land here: the task failed (rejected, or threw before
          // returning above), or the handler above threw after it had already
          // recycled the worker. `slotReturned` tells them apart — only the
          // first leaves a worker unaccounted for.
          const abandoned = slotReturned ? null : worker

          // The lent worker is dropped rather than recycled: a failed task
          // leaves it in an unknown state, and a NodeWorker whose thread died
          // latches the error and replays it to every later request, so
          // handing it to the next task would poison the slot. An empty slot
          // takes its place and the next task creates a fresh worker.
          returnSlot(null)

          info!.reject!(error)
          this.clearTask(info!.index)

          // Nothing else holds a reference to the abandoned worker, so this is
          // its only chance to be shut down — a leaked `node:worker_threads`
          // thread keeps the whole process alive.
          abandoned?.terminate()
        })
    } else {
      if (info!.runningWorkers !== 0 || info!.postponed) {
        // A running worker will pick up the next item when it completes.
        info!.taskQueue.push([resultIndex, task as WorkerPoolTask<unknown>] as [number, WorkerPoolTask<T>])
      } else {
        // Retry after a short delay.
        info!.postponed = true
        setTimeout(() => {
          info!.postponed = false
          this.addTask<T>(info!.index, resultIndex, task)
        }, 50)
      }
    }
  }

  /**
   * Clean up a completed/canceled run's bookkeeping without removing it from
   * the array (indices are used as run IDs).
   * @internal
   */
  private clearTask(clearIndex: number): void {
    const info = this.runInfo[clearIndex]
    info.cleared = true
    info.results = []
    info.taskQueue = []
    info.progressCallback = null
    info.canceled = null
    info.reject = () => {}
    info.resolve = () => {}
  }
}

export default WorkerPool
