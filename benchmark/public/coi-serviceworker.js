/*! coi-serviceworker v0.1.7 - Guido Zuidhof, licensed under MIT */
/*
 * This service worker intercepts all fetch requests and adds
 * Cross-Origin-Embedder-Policy: require-corp
 * Cross-Origin-Opener-Policy: same-origin
 * headers to enable SharedArrayBuffer on environments like GitHub Pages
 * that don't allow custom HTTP headers.
 */
if (typeof window === 'undefined') {
  // Service worker context
  self.addEventListener('install', () => self.skipWaiting())
  self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()))
  self.addEventListener('fetch', (e) => {
    if (
      e.request.cache === 'only-if-cached' &&
      e.request.mode !== 'same-origin'
    ) {
      return
    }
    e.respondWith(
      fetch(e.request).then((r) => {
        if (r.status === 0) return r
        const headers = new Headers(r.headers)
        headers.set('Cross-Origin-Embedder-Policy', 'require-corp')
        headers.set('Cross-Origin-Opener-Policy', 'same-origin')
        return new Response(r.body, {
          status: r.status,
          statusText: r.statusText,
          headers,
        })
      }),
    )
  })
} else {
  // Window context — register the service worker
  ;(async () => {
    if (window.crossOriginIsolated !== false) return

    const registration = await navigator.serviceWorker.register(
      window.document.currentScript.src,
    )
    if (registration.active && !navigator.serviceWorker.controller) {
      window.location.reload()
    } else if (!registration.active) {
      registration.addEventListener('updatefound', () => {
        registration.installing?.addEventListener('statechange', () => {
          if (registration.active && !navigator.serviceWorker.controller) {
            window.location.reload()
          }
        })
      })
    }
  })()
}
