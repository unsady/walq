# Contributing

## Development

Walq requires Node.js 22 or newer and pnpm 12.

```sh
pnpm install
pnpm check
```

Add a changeset for every user-visible package change:

```sh
pnpm changeset
```

Commits should keep the public storage contract, package READMEs, and tests in sync. Benchmark changes should include their methodology and should not be used as release gates unless explicitly documented.
