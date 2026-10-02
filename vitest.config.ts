import { fileURLToPath } from 'node:url'

import { defineConfig } from 'vitest/config'

function source(path: string): string {
  return fileURLToPath(new URL(path, import.meta.url))
}

export default defineConfig({
  resolve: {
    alias: [
      {
        find: /^@walq\/sqlite-common$/,
        replacement: source('./packages/sqlite-common/src/index.ts'),
      },
      { find: /^@walq\/sqlite$/, replacement: source('./packages/sqlite/src/index.ts') },
      { find: /^@walq\/core\/storage$/, replacement: source('./packages/core/src/storage.ts') },
      { find: /^@walq\/core$/, replacement: source('./packages/core/src/index.ts') },
      {
        find: /^@walq\/better-sqlite3$/,
        replacement: source('./packages/better-sqlite3/src/index.ts'),
      },
    ],
  },
  test: {
    environment: 'node',
    include: ['packages/*/src/**/*.test.ts', 'benchmarks/**/*.test.ts'],
  },
})
