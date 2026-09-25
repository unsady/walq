# Releasing

The two public packages are released together with the same version:

- `@walq/core`
- `@walq/better-sqlite3`

## One-time setup

1. Create or obtain publish access to the public `@walq` npm scope.
2. Configure both packages in npm to trust `.github/workflows/release.yml` in `unsady/walq`, using the `npm` GitHub environment.
3. Leave direct `npm publish` disabled so releases must use staged publishing.
4. Protect the `npm` environment and the `v*` tag pattern as appropriate.

## Prepare a version

1. Add changesets with `pnpm changeset` as user-visible changes are merged.
2. Run `pnpm version-packages` on a release branch.
3. Review both package versions, dependency ranges, generated changelogs, and migration notes.
4. Run `pnpm clean && pnpm check` from a fresh install.
5. Merge the version changes into `main`.

## Publish

Before tagging, add the GitHub release notes to `release-notes/<tag>.md`, for example `release-notes/v0.2.2.md`.

Create and push a version tag matching both manifests:

```sh
git tag v0.1.0
git push origin v0.1.0
```

The workflow validates the tag, tests packed artifacts in a clean consumer project, stages both packages through npm Trusted Publishing, and creates a published GitHub release using the matching notes file.

After the workflow succeeds:

1. Review the staged packages in npm.
2. Approve `@walq/core` first and `@walq/better-sqlite3` second, confirming each action with 2FA.
3. Install both packages by exact version in a separate project and run the README example against a file-backed database.
