import { defineConfig } from 'oxfmt'

export default defineConfig({
  tabWidth: 2,
  singleQuote: true,
  semi: false,
  sortImports: true,
  ignorePatterns: ['dist/**', 'coverage/**', '.vitest/**', 'pnpm-lock.yaml'],
})
