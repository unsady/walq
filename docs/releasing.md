# Releasing

The public packages are:

- `@walq/core`
- `@walq/sqlite-common`
- `@walq/better-sqlite3`
- `@walq/sqlite`

All four packages are versioned together. Release tags must match every package version.

## One-time setup

1. Create or obtain publish access to the public `@walq` npm scope.
2. Configure all four packages in npm to trust `.github/workflows/release.yml` in `unsady/walq`, using the `npm` GitHub environment. Configure the new `@walq/sqlite-common` and `@walq/sqlite` packages before their first release.
3. Leave direct `npm publish` disabled so releases must use staged publishing.
4. Protect the `npm` environment and the `v*` tag pattern as appropriate.

## Prepare a version

1. Add changesets with `pnpm changeset` as user-visible changes are merged.
2. Create a release branch from the current `main`; run `pnpm version-packages` and `pnpm install --lockfile-only` on that branch.
3. Review all package versions, dependency ranges, generated changelogs, and migration notes.
4. Add the GitHub release notes to `release-notes/<tag>.md`, listing each package version.
5. Run `pnpm install --frozen-lockfile && pnpm clean && pnpm check`.
6. Open a pull request and merge the version changes into `main` after checks pass. Do not push release commits directly to `main`.

## Publish

From the merged release commit on `main`, create and push a version tag matching all four package manifests:

```sh
git switch main
git pull --ff-only
git tag v1.1.0
git push origin v1.1.0
```

The workflow validates the tag, tests packed artifacts in clean consumer projects, stages all unpublished package versions through npm Trusted Publishing, and creates a published GitHub release using the matching notes file.

After the workflow succeeds:

1. Review the staged packages in npm.
2. Approve packages in dependency order: `@walq/core`, `@walq/sqlite-common`, then `@walq/better-sqlite3` and `@walq/sqlite`, confirming each action with 2FA. Skip versions already published.
3. Install core and each adapter by exact version in separate projects and run their README examples against file-backed databases.
