import { defineConfig } from 'oxlint'

export default defineConfig({
  plugins: ['typescript', 'unicorn', 'oxc', 'vitest'],
  categories: {
    correctness: 'error',
  },
  rules: {
    'func-style': ['error', 'declaration'],
    'typescript/consistent-type-definitions': ['error', 'interface'],
  },
  ignorePatterns: ['dist/**', 'coverage/**', '.vitest/**'],
})
