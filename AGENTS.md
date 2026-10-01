# Repository context

- Walq is an ESM TypeScript queue runtime with a separate SQLite adapter.
- Run `pnpm check` to validate the workspace.

## Code style

- Enforce mechanical style through oxfmt and oxlint.
- Use `interface` for object shapes, including callable contracts; use `type` for other types.
- Use function declarations for named functions; when lexical `this` requires an arrow, suppress lint narrowly and explain why.
- Use arrow functions for inline callbacks, including object options; avoid object method shorthand for callbacks.

## Maintenance

- Add a changeset with `pnpm changeset` for every user-visible package change.
- Extend existing tests and tooling instead of adding parallel implementations.
- Keep storage behavior in conformance tests and SQLite implementation details in adapter tests.
- Use the shared benchmark harness for maintained performance suites.
- Keep completed experiments only when they serve an ongoing comparison; otherwise retain the decision, not the runner.
- Keep READMEs focused on usage; link to the storage contract for adapter semantics and release notes for migration history.
