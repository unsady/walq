import { defineConfig } from 'vitest/config'

export default defineConfig({
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
