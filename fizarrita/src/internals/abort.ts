/**
 * AbortSignal plumbing shared by getWorker, setWorker and the sharded-chunk
 * reader: separating a caller's signal from store options that a *shared*
 * request may carry, racing a shared promise against a caller's signal, and
 * combining two signals.
 */

export function isAbortSignal(value: unknown): value is AbortSignal {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as AbortSignal).aborted === "boolean" &&
    typeof (value as AbortSignal).addEventListener === "function"
  )
}

/**
 * Split a caller's store options into what a *shared* store request may carry
 * and the caller's own `AbortSignal`, if the options hold one.
 *
 * Everything else — headers, credentials, cache mode — describes how to talk
 * to the store and is the same for every caller of the same store, so it can
 * safely govern a request made on behalf of all of them. A signal is the one
 * option that belongs to a single caller: it says "I no longer want this",
 * which is not a statement the others have made.
 *
 * Only a real signal is separated. Options with no `signal`, or one that is
 * not an `AbortSignal`, are passed through untouched — same object, no copy.
 */
export function separateSignal<Opts>(storeOpts: Opts): {
  shared: Opts
  signal: AbortSignal | undefined
} {
  if (storeOpts && typeof storeOpts === "object" && "signal" in storeOpts) {
    const { signal, ...shared } = storeOpts as Opts & { signal?: unknown }
    if (isAbortSignal(signal)) {
      return { shared: shared as Opts, signal }
    }
  }
  return { shared: storeOpts, signal: undefined }
}

/**
 * Settle as `promise` does, unless `signal` aborts first — then reject with
 * the abort reason, exactly as a fetch given that signal would. `promise`
 * itself is untouched and keeps running for whoever else awaits it.
 *
 * Whatever the outcome, `promise` is observed here: once the caller has been
 * rejected on the signal's account, this is the only place still watching
 * the promise it was handed, and a promise that later rejects with nobody
 * watching is an unhandled rejection — fatal under Node's default.
 */
export function untilAborted<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T> {
  if (!signal) return promise
  const reason = () =>
    signal.reason ?? new DOMException("The operation was aborted.", "AbortError")
  if (signal.aborted) {
    // The caller never sees `promise` — a fresh derived promise at both call
    // sites — so absorb its outcome rather than leave a rejection unhandled.
    // Other observers of the same chain are unaffected by this.
    promise.catch(() => {})
    return Promise.reject(reason())
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(reason())
    signal.addEventListener("abort", onAbort, { once: true })
    const settled = () => signal.removeEventListener("abort", onAbort)
    promise.then(
      (value) => {
        settled()
        resolve(value)
      },
      (error) => {
        settled()
        reject(error)
      },
    )
  })
}

/**
 * Combine two abort signals into one that fires when either does.
 *
 * `AbortSignal.any` where available; on runtimes that predate it (Safari
 * before 17.4, Node before 20.3) a controller bridge. The bridge's listeners
 * stay on the parent signals for the parents' lifetime — acceptable for
 * one-shot read signals, which is the only way this module uses them.
 */
export function combineAbortSignals(a: AbortSignal, b: AbortSignal): AbortSignal {
  if (typeof AbortSignal.any === "function") {
    return AbortSignal.any([a, b])
  }
  if (a.aborted) return a
  if (b.aborted) return b
  const controller = new AbortController()
  const forward = (signal: AbortSignal) => {
    signal.addEventListener("abort", () => controller.abort(signal.reason), {
      once: true,
    })
  }
  forward(a)
  forward(b)
  return controller.signal
}
