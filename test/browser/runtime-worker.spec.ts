import { expect, test } from '@playwright/test'

/**
 * The browser half of the runtime-aware worker factories. The Node half lives
 * in `test/node/` — between them, both branches of `createWorker` and
 * `createDefaultWorker` are covered.
 */
test.describe('runtime-aware worker creation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('isNodeRuntime is false in the browser', async ({ page }) => {
    expect(await page.evaluate(() => window.isNodeRuntime())).toBe(false)
  })

  test('createWorker returns a real Worker where one exists', async ({ page }) => {
    const result = await page.evaluate(async () => {
      const worker = window.createWorker(window.testWorkerUrl)
      try {
        const isWorker = worker instanceof Worker
        const value = await new Promise<number>((resolve, reject) => {
          worker.addEventListener('message', (e: MessageEvent<{ result: number }>) =>
            resolve(e.data.result),
          )
          worker.addEventListener('error', () => reject(new Error('worker error')))
          worker.postMessage({ value: 12, delay: 0 })
        })
        return { isWorker, value }
      } finally {
        worker.terminate()
      }
    })

    expect(result.isWorker).toBe(true)
    expect(result.value).toBe(144)
  })

  test('createWorker drives the pool like a hand-built Worker', async ({ page }) => {
    const results = await page.evaluate(async () => {
      const pool = new window.WorkerPool(2)
      try {
        const tasks = [2, 3, 4].map((value) => async (slot: unknown) => {
          const worker = slot ?? window.createWorker(window.testWorkerUrl)
          const result = await new Promise<number>((resolve, reject) => {
            const w = worker as Worker
            const onMessage = (e: MessageEvent<{ result: number }>) => {
              w.removeEventListener('message', onMessage)
              resolve(e.data.result)
            }
            w.addEventListener('message', onMessage)
            w.addEventListener('error', () => reject(new Error('worker error')))
            w.postMessage({ value, delay: 0 })
          })
          return { worker, result }
        })
        const { promise } = pool.runTasks(tasks as never)
        return (await promise) as number[]
      } finally {
        pool.terminateWorkers()
      }
    })

    expect(results).toEqual([4, 9, 16])
  })

  test('fizarrita createDefaultWorker speaks the codec protocol', async ({ page }) => {
    const reply = await page.evaluate(async () => {
      const worker = window.createDefaultWorker()
      try {
        return await new Promise<{ type: string; id: number }>((resolve, reject) => {
          worker.addEventListener('message', (e: MessageEvent) => resolve(e.data))
          worker.addEventListener('error', () => reject(new Error('worker error')))
          worker.postMessage({
            type: 'init',
            id: 1,
            metaId: 4242,
            meta: {
              data_type: 'int32',
              chunk_shape: [2, 2],
              codecs: [{ name: 'bytes', configuration: { endian: 'little' } }],
            },
          })
        })
      } finally {
        worker.terminate()
      }
    })

    expect(reply.type).toBe('init_ok')
    expect(reply.id).toBe(1)
  })
})
