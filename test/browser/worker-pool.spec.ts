import { test, expect } from '@playwright/test'

test.describe('WorkerPool', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('add + onIdle: returns results in order', async ({ page }) => {
    const results = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      const values = [2, 3, 5, 7]
      for (const v of values) {
        pool.add(window.createSquareTask(v))
      }
      return pool.onIdle<number>()
    })

    expect(results).toEqual([4, 9, 25, 49])
  })

  test('add + onIdle: handles empty task list', async ({ page }) => {
    const results = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      return pool.onIdle<number>()
    })

    expect(results).toEqual([])
  })

  test('concurrency limiting: pool of 2 handles 4 tasks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      const values = [1, 2, 3, 4]
      for (const v of values) {
        // Add a 50ms delay so tasks overlap and queue up
        pool.add(window.createSquareTask(v, 50))
      }
      const results = await pool.onIdle<number>()
      pool.terminateWorkers()
      return results
    })

    expect(result).toEqual([1, 4, 9, 16])
  })

  test('worker recycling: workers are reused across tasks', async ({ page }) => {
    const workerCount = await page.evaluate(async () => {
      let workersCreated = 0
      const realWorkerUrl = window.testWorkerUrl

      // Patch the task factory to count worker creations
      function trackingSquareTask(value: number) {
        return (worker: Worker | null): Promise<{ worker: Worker; result: number }> => {
          const w = worker ?? (() => { workersCreated++; return new Worker(realWorkerUrl, { type: 'module' }) })()
          return new Promise((resolve, reject) => {
            w.onmessage = (event: MessageEvent<{ result: number }>) => {
              resolve({ worker: w, result: event.data.result })
            }
            w.onerror = (err: ErrorEvent) => reject(err)
            w.postMessage({ value, delay: 0 })
          })
        }
      }

      const pool = new window.WorkerPool(2)
      // Run 6 tasks with pool size 2 — should only create 2 workers
      for (let i = 1; i <= 6; i++) {
        pool.add(trackingSquareTask(i))
      }
      await pool.onIdle<number>()
      pool.terminateWorkers()
      return workersCreated
    })

    expect(workerCount).toBe(2)
  })

  test('runTasks with progress callback', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      const values = [2, 3, 5, 7]
      const taskFns = values.map(v => window.createSquareTask(v))

      const progressUpdates: Array<[number, number]> = []
      function onProgress(completed: number, total: number) {
        progressUpdates.push([completed, total])
      }

      const { promise } = pool.runTasks(taskFns, onProgress)
      const results = await promise
      pool.terminateWorkers()

      return { results, progressUpdates }
    })

    expect(result.results).toEqual([4, 9, 25, 49])
    // Progress was reported for each of the 4 tasks
    expect(result.progressUpdates).toHaveLength(4)
    // Every update should have total = 4
    for (const [, total] of result.progressUpdates) {
      expect(total).toBe(4)
    }
    // The last update should report 4 completed
    const lastCompleted = result.progressUpdates.map(([c]) => c).sort((a, b) => a - b)
    expect(lastCompleted[lastCompleted.length - 1]).toBe(4)
  })

  test('runTasks cancellation', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const pool = new window.WorkerPool(1) // single worker to force queuing

      // 4 tasks with 100ms delay each on 1 worker = sequential
      const taskFns = [1, 2, 3, 4].map(v => window.createSquareTask(v, 100))

      const { promise, runId } = pool.runTasks(taskFns)

      // Cancel after 150ms — the first task should be running/done,
      // remaining should get canceled
      setTimeout(() => pool.cancel(runId), 150)

      try {
        await promise
        return { rejected: false, error: '' }
      } catch (err) {
        return { rejected: true, error: String(err) }
      } finally {
        pool.terminateWorkers()
      }
    })

    expect(result.rejected).toBe(true)
    expect(result.error).toContain('Remaining tasks canceled')
  })

  test('terminateWorkers: pool creates fresh workers after termination', async ({ page }) => {
    const results = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)

      // First batch
      pool.add(window.createSquareTask(3))
      pool.add(window.createSquareTask(4))
      const first = await pool.onIdle<number>()

      // Terminate
      pool.terminateWorkers()

      // Second batch — should work with fresh workers
      pool.add(window.createSquareTask(5))
      pool.add(window.createSquareTask(6))
      const second = await pool.onIdle<number>()

      pool.terminateWorkers()
      return { first, second }
    })

    expect(results.first).toEqual([9, 16])
    expect(results.second).toEqual([25, 36])
  })

  test('multiple onIdle batches: pool can be reused', async ({ page }) => {
    const results = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)

      // Batch 1
      pool.add(window.createSquareTask(2))
      pool.add(window.createSquareTask(3))
      const batch1 = await pool.onIdle<number>()

      // Batch 2 (reusing same pool and workers)
      pool.add(window.createSquareTask(10))
      pool.add(window.createSquareTask(11))
      const batch2 = await pool.onIdle<number>()

      pool.terminateWorkers()
      return { batch1, batch2 }
    })

    expect(results.batch1).toEqual([4, 9])
    expect(results.batch2).toEqual([100, 121])
  })

  test('error handling: task rejection propagates to onIdle', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      pool.add(window.createSquareTask(2))
      pool.add(window.createFailingTask())

      try {
        await pool.onIdle<number>()
        return { rejected: false, error: '' }
      } catch (err) {
        return { rejected: true, error: err instanceof Error ? err.message : String(err) }
      } finally {
        pool.terminateWorkers()
      }
    })

    expect(result.rejected).toBe(true)
    expect(result.error).toContain('intentional failure')
  })

  test('error handling: task rejection propagates to runTasks', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      const taskFns = [
        window.createSquareTask(2),
        window.createFailingTask(),
      ]

      try {
        await pool.runTasks(taskFns).promise
        return { rejected: false, error: '' }
      } catch (err) {
        return { rejected: true, error: err instanceof Error ? err.message : String(err) }
      } finally {
        pool.terminateWorkers()
      }
    })

    expect(result.rejected).toBe(true)
    expect(result.error).toContain('intentional failure')
  })

  test('large batch: pool of 2 handles 20 tasks correctly', async ({ page }) => {
    const results = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      const values = Array.from({ length: 20 }, (_, i) => i + 1)
      for (const v of values) {
        pool.add(window.createSquareTask(v))
      }
      const results = await pool.onIdle<number>()
      pool.terminateWorkers()
      return results
    })

    const expected = Array.from({ length: 20 }, (_, i) => (i + 1) ** 2)
    expect(results).toEqual(expected)
  })
})
