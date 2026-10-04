# Releasing

Changesets manages versions, internal dependency ranges, package changelogs, the Version Packages PR, and npm publication. The four public packages share one version through the `fixed` group in `.changeset/config.json`:

- `@walq/core`
- `@walq/sqlite-common`
- `@walq/better-sqlite3`
- `@walq/sqlite`

## One-time setup

1. Install a GitHub App on `unsady/walq` with repository **Contents: read and write** and **Pull requests: read and write** permissions. Set the repository variable `RELEASE_APP_ID` and secret `RELEASE_APP_PRIVATE_KEY`. The version job uses this App so its PRs and updates trigger ordinary `pull_request` CI; PRs created with the default `GITHUB_TOKEN` do not trigger those workflows.
2. Protect `main` with required PR checks, including the checks in `.github/workflows/ci.yml`. Merge Version Packages PRs through the same checks as feature PRs; the release workflow does not rerun lint, typechecking, or tests after merge.
3. Configure each package's npm Trusted Publisher for GitHub Actions: owner `unsady`, repository `walq`, workflow `release.yml`, environment `npm`. The packages must already exist on npm before their Trusted Publishers can be configured.
4. Enable direct publishing through Trusted Publishing in each package's npm settings. Do not require staged publication or manual npm approval. No `NPM_TOKEN` or `NODE_AUTH_TOKEN` is needed; the publish job receives short-lived OIDC credentials through `id-token: write`.
5. Configure the GitHub `npm` environment to allow `main`. Leave required reviewers disabled for fully automatic publication. Only the publish job uses this environment; Version Packages PR creation never waits for npm environment approval.

## Release flow

1. Add a changeset with `pnpm changeset` in each feature PR that changes a public package. Describe the change and select the appropriate semver bump.
2. Run and pass PR CI, then merge the feature PR into `main`.
3. `changesets/action` creates or updates **Version Packages**. It runs `pnpm version-packages`, whose package script executes `changeset version` followed by `pnpm install --lockfile-only --no-frozen-lockfile`, committing the versions, internal dependencies, package changelogs, and lockfile together.
4. Review and merge Version Packages after ordinary PR CI passes. Do not edit versions manually or create a release tag yourself.
5. The publish job runs `pnpm changeset publish` using npm Trusted Publishing. Each package's existing `prepack` script builds it during publication; no separate build step is needed. Changesets determines which versions need publication; there is no staged publishing or manual npm approval.
6. After `changeset publish` succeeds, the workflow takes the shared version from `packages/core/package.json` and creates one GitHub Release and tag, `vX.Y.Z`, targeting that release commit. Its notes combine the matching sections of the four package changelogs. A package without a section is noted as version alignment only.

`createGithubReleases: false` disables Changesets' package-specific GitHub Releases and prevents the action from pushing package tags. Changesets creates those tags locally; only the combined `vX.Y.Z` tag and GitHub Release are created on GitHub by the final step. No separate release-note files are maintained. The final step skips an existing GitHub Release, so rerunning a failed release run can create a missing release even when npm publication already completed.

The release workflow runs on pushes to `main`; CI runs only on `pull_request`. Release runs are serialized. Pushes without pending changesets use Changesets' normal publish behavior, which skips versions already present on npm.
