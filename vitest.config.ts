import { fileURLToPath } from 'node:url'

import { defineConfig } from 'vitest/config'

const source = (path: string): string => fileURLToPath(new URL(path, import.meta.url))

export default defineConfig({
  resolve: {
    alias: [
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
    benchmark: {
      include: ['benchmarks/**/*.bench.ts'],
      provider: 'benchmarks/vitest-provider.ts',
      retainSamples: false,
    },
  },
})
