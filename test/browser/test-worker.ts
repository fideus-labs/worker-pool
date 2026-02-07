/**
 * A simple test worker that squares a number after an optional delay.
 *
 * Message protocol:
 *   Request:  { value: number, delay?: number }
 *   Response: { result: number }
 */

const ctx = self as unknown as DedicatedWorkerGlobalScope

ctx.addEventListener('message', (event: MessageEvent<{ value: number; delay?: number }>) => {
  const { value, delay = 0 } = event.data

  if (delay > 0) {
    setTimeout(() => {
      ctx.postMessage({ result: value * value })
    }, delay)
  } else {
    ctx.postMessage({ result: value * value })
  }
})
