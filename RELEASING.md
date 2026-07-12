# Releasing

This package publishes to npm as **`@mpgxc/nestjs-sqs-consumer`** using
**GitHub Actions + npm OIDC trusted publishing** (no long-lived token in CI).

There is a one-time bootstrap (npm requires a package to exist before a trusted
publisher can be linked to it), then every release is just a GitHub Release.

## Prerequisites

- An npm account that owns the `@mpgxc` scope (your npm username is a scope).
- Maintainer access to `mpgxc/nestjs-sqs-consumer` on GitHub.
- `#12` (npm publishing config) merged into `master`.

## One-time bootstrap (first publish)

Provenance requires OIDC, which only exists in CI — so the **first** publish is
done locally with provenance disabled, just to create the package on npm.

```bash
git checkout master && git pull
pnpm install
pnpm run check        # typecheck + lint + unit
pnpm run build

npm login             # or: npm config set //registry.npmjs.org/:_authToken <token>

# version is a prerelease (4.0.0-alpha.x) → publish under the "alpha" dist-tag,
# NOT "latest", and skip provenance for this local run.
npm publish --access public --tag alpha --provenance=false
```

Then link the trusted publisher on npm:

1. npmjs.com → **@mpgxc/nestjs-sqs-consumer** → **Settings** → **Trusted Publisher**.
2. Add a **GitHub Actions** publisher:
   - Repository: `mpgxc/nestjs-sqs-consumer`
   - Workflow filename: `publish.yml`
   - Environment: *(leave empty unless you add one)*
3. Save. From now on CI can publish without any token.

## Every subsequent release (CI, no token)

1. Bump the version on `master` (choose one):
   ```bash
   pnpm version 4.0.0-alpha.6 --no-git-tag-version   # next prerelease
   pnpm version 4.0.0 --no-git-tag-version           # first stable
   ```
   Commit it (via a PR, per the repo convention).
2. Create a **GitHub Release** whose tag matches the version, e.g. `v4.0.0-alpha.6`
   (the workflow strips a leading `v`).
3. The `publish.yml` workflow then:
   - runs typecheck / lint / unit / e2e on Node 22 & 24,
   - builds,
   - resolves the dist-tag from the version (`-alpha`/`-beta`/`-rc` → that tag,
     otherwise `latest`),
   - runs `npm publish --provenance --access public --tag <dist-tag>` via OIDC.

## dist-tags

- Prereleases (`4.0.0-alpha.5`, `4.0.0-rc.1`, …) publish under `alpha` / `beta` /
  `rc` — installable with `npm i @mpgxc/nestjs-sqs-consumer@alpha`, and they do
  **not** move `latest`.
- A plain version (`4.0.0`) publishes under `latest`.

## Verifying a release

```bash
npm view @mpgxc/nestjs-sqs-consumer dist-tags
npm view @mpgxc/nestjs-sqs-consumer@alpha
```

Provenance appears on the package page on npmjs.com once published from CI.
