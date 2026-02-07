import { defineConfig } from 'vite'

export default defineConfig({
  root: 'test/app',
  server: {
    port: 5173,
    strictPort: true,
  },
})
