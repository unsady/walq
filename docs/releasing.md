# Releasing

The two public packages are released together with the same version:

- `walq`
- `@walq/better-sqlite3`

## One-time setup

1. Confirm that the `walq` package name is available to the publisher.
2. Create or obtain publish access to the public `@walq` npm scope.
3. For the first publication, add a short-lived or granular automation token as the `NPM_TOKEN` secret in the `npm` GitHub environment. npm cannot configure a trusted publisher for a package that does not exist yet.
4. After both packages exist, configure them in npm to trust `.github/workflows/release.yml` in `unsady/walq`, using the `npm` GitHub environment.
5. Remove `NPM_TOKEN`; subsequent releases use GitHub OIDC and npm provenance without a long-lived token.
6. Protect the `npm` environment and the `v*` tag pattern as appropriate.

## Prepare a version

1. Add changesets with `pnpm changeset` as user-visible changes are merged.
2. Run `pnpm version-packages` on a release branch.
3. Review both package versions, dependency ranges, generated changelogs, and migration notes.
4. Run `pnpm clean && pnpm check` from a fresh install.
5. Merge the version changes into `main`.

During `0.x`, document every incompatible API or SQLite schema change. If no migration is provided, explicitly tell users that they must drain or recreate the database before upgrading.

## Publish

Create and push a version tag matching both manifests:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The workflow validates the tag, tests packed artifacts in a clean consumer project, publishes `walq` first, publishes the adapter second, and creates a GitHub release. Publishing is retryable if only the first package succeeds.

Afterward, install both packages by exact version in a separate project and run the README example against a file-backed database.
