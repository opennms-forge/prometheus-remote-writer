# Releasing

This document describes the release procedure for `prometheus-remote-writer`.
It assumes the `main` branch is in a releasable state (CI green, `make verify`
passing locally, KAR install verified against a Horizon 35 target).

## Summary

Releases are driven by **pushing a `v*.*.*` git tag**. The
[`release.yml`](.github/workflows/release.yml) workflow picks up the tag push,
builds the KAR, extracts the matching section from `CHANGELOG.md`, and
creates a GitHub Release with the KAR attached.

Tags follow [SemVer](https://semver.org): `vMAJOR.MINOR.PATCH` (`v0.1.0`,
`v0.2.0`, `v1.0.0-rc1`).

## Pre-flight checklist

Before tagging, confirm:

- [ ] `main` is on the commit you want to ship.
- [ ] `make verify` is green locally (unit tests + Testcontainers integration
      test against a real Prometheus container).
- [ ] CI is green on the commit being tagged.
- [ ] `CHANGELOG.md` has a `## [X.Y.Z]` section for the target version with
      the changes since the previous release. Move `[Unreleased]` content
      into the new version section if needed.
- [ ] The `<version>` in `pom.xml` matches the tag (without the `v` prefix).
      For `v0.1.0` the version is `0.1.0`; once shipped, bump the pom to
      the next `-SNAPSHOT` on `main`.
- [ ] `README.md` Quick-start references match the target version.

## Release steps

### 1. Update version and CHANGELOG

If the pom is still on `0.1.0-SNAPSHOT` and you're cutting `v0.1.0`, strip the
`-SNAPSHOT`:

```bash
./mvnw versions:set -DnewVersion=0.1.0 -DgenerateBackupPoms=false
git diff   # sanity-check
```

Edit `CHANGELOG.md`:

- Move content under `## [Unreleased]` into a new `## [0.1.0] — YYYY-MM-DD`
  section.
- Add a fresh empty `## [Unreleased]` at the top.
- Update the comparison links at the bottom of the file.

Commit both changes together:

```bash
git add pom.xml CHANGELOG.md
git commit -m "release: v0.1.0"
```

### 2. Tag the release

```bash
git tag -a v0.1.0 -m "v0.1.0"
git push origin main
git push origin v0.1.0
```

Pushing the tag triggers `.github/workflows/release.yml`:

- Builds the KAR via `make kar`.
- Extracts the `## [0.1.0]` section from `CHANGELOG.md` as the release body.
- Creates a GitHub Release named `v0.1.0` with the KAR attached as an asset.

Watch the run:

```bash
gh run watch --repo labmonkeys-space/prometheus-remote-writer
```

### 3. Post-release

Bump `main` to the next development version:

```bash
./mvnw versions:set -DnewVersion=0.2.0-SNAPSHOT -DgenerateBackupPoms=false
git add pom.xml
git commit -m "chore: bump to 0.2.0-SNAPSHOT"
git push origin main
```

Announce the release (release-notes URL from the GitHub Releases page).

## Re-running a release

If the workflow fails after tag push (bad CHANGELOG, transient CI issue), you
can re-run it manually via the `workflow_dispatch` trigger:

- GitHub UI → Actions → **Release** → **Run workflow** → enter the tag name.

Or via CLI:

```bash
gh workflow run release.yml -f tag=v0.1.0
```

This is idempotent — `gh release create` will fail if the release already
exists, so delete the previous release and its asset first if you're
recovering from a partial run.

## Hotfix releases

For a patch release (e.g. `v0.1.1`) on top of `v0.1.0`:

1. Branch off the previous tag: `git checkout -b hotfix/0.1.1 v0.1.0`.
2. Apply the fix, commit, update CHANGELOG and pom version.
3. Merge back to `main` (or cherry-pick).
4. Tag `v0.1.1` on the hotfix branch.
5. Push the tag.

## What gets published

| Artifact | Where | How consumed |
|---|---|---|
| `prometheus-remote-writer-kar-<version>.kar` | GitHub Release asset | Download and install via Karaf `kar:install <path>` |
| `prometheus-remote-writer-<version>.jar` (bundle) | Not auto-published in v0.1 | Planned once the repo moves under the OpenNMS namespace and a Maven repo target is chosen |
| `prometheus-remote-writer-features-<version>-features.xml` | Not auto-published in v0.1 | Same as above — consumed via `feature:repo-add mvn:…/xml/features` when a Maven repo is available |

## Deferred to a later release

- **Maven artifact publication** — required for the Karaf
  `feature:repo-add mvn:…` install flow shown in the README. Pending the
  repo's migration under the OpenNMS namespace and a Maven-repo target
  decision (Central via Sonatype, GitHub Packages, or a private Nexus).
- **Release signing** — GPG-signed tags and signed Maven artifacts.
- **Provenance attestation** — SLSA-style build provenance is a v0.2+ goal.
