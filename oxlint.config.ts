import { defineConfig } from 'oxlint'

export default defineConfig({
  plugins: ['typescript', 'unicorn', 'oxc', 'vitest'],
  categories: {
    correctness: 'error',
  },
  ignorePatterns: ['dist/**', 'coverage/**', '.vitest/**'],
})
