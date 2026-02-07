import { defineConfig } from 'vite'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const repoRoot = resolve(__dirname, '..')

export default defineConfig({
  root: '.',
  server: {
    port: 5174,
    strictPort: true,
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
    fs: {
      // Allow serving files from the entire monorepo
      allow: [repoRoot],
    },
  },
  resolve: {
    alias: {
      // Map workspace package imports to TypeScript source
      '@fideus-labs/worker-pool': resolve(repoRoot, 'src/index.ts'),
      '@fideus-labs/fizarrita': resolve(repoRoot, 'fizarrita/src/index.ts'),
      // Allow direct relative imports into repo source via $root
      '$root': repoRoot,
    },
  },
})
