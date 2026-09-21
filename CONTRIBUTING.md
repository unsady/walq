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

## Code style

Formatting and mechanically enforceable style belong in oxfmt and oxlint rather
than review comments.

- Use `interface` for object shapes, including callable or method-bearing
  contracts.
- Use `type` for unions, intersections, tuples, mapped or conditional types,
  function signatures, and aliases of non-object types.
- Use function declarations for named functions, whether exported or local.
- Use arrow functions for inline callbacks, including callbacks passed as
  object options; do not use object method shorthand for them. If lexical
  `this` makes a named arrow necessary, use a narrow lint suppression and
  explain why.

Commits should keep the public storage contract, package READMEs, and tests in sync. Benchmark changes should include their methodology and should not be used as release gates unless explicitly documented.
