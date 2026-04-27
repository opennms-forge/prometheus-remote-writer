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

## Release signing key

The project signs every release with a single project-specific GPG key.
The same key signs the git tag (locally on the maintainer's laptop) and
the release artifacts (in CI from **organization-scoped secrets**
`PRW_GPG_PRIVATE_KEY`, `PRW_GPG_PASSPHRASE`, `PRW_GPG_KEY_ID` set at the
`opennms-forge` org level, visible to all public repositories in the
org). The workflow accesses them via the standard
`${{ secrets.PRW_GPG_* }}` syntax — GitHub merges org-level and
repo-level secrets transparently. To inspect or rotate them: `gh secret
list --org opennms-forge | grep PRW_GPG`.

| Field | Value |
|---|---|
| UID | `prometheus-remote-writer release signing (opennms-forge) <ronny@no42.org>` |
| Algorithm | RSA 4096 |
| Long key ID | `0x1FC793D7F2E3FDDD` |
| Fingerprint | `53BC D4E3 C0CC 9ACF 40F4  6669 1FC7 93D7 F2E3 FDDD` |
| Validity | `2026-04-27 through 2028-04-26` (2-year expiry) |

**This is the canonical fingerprint.** Cross-check against the `KEYS`
file on any GitHub Release before trusting that release's signatures —
the on-release `KEYS` is convenience; this fingerprint is the trust anchor.

### Key rotation

Keys can leak, expire, or need replacing. The procedure (manual, but
documented so it's not invented under pressure):

1. Generate a new keypair following the same UID convention. Document the
   new long key ID + fingerprint in this section, **alongside** the old
   one — do not remove the old entry; old releases were signed by it and
   verifiers need it to keep working.
2. Export the new public key and append it to a fresh `KEYS` file. The
   release workflow exports just the active key; rotation transitions
   are visible in this README, not in the on-release `KEYS`.
3. Update the three GitHub secrets to the new key.
4. Cut a transition release whose CHANGELOG entry calls out the rotation
   and includes both fingerprints in the release notes.
5. Retire the old key after a transition window (suggested ≥ 30 days).
   Mark the old fingerprint as `(retired YYYY-MM-DD)` in the table above.

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
- [ ] **The git tag will be GPG-signed** — use `git tag -s vX.Y.Z -m "vX.Y.Z"`
      (NOT `git tag` plain or `git tag -a`). The `release.yml` workflow
      verifies the tag signature and refuses to publish if it's missing.
- [ ] **The signing key matches the project key** — the fingerprint
      reported by `git tag -v vX.Y.Z` MUST equal the canonical one in
      "Release signing key" above. A personal key may sign in-tree
      commits, but release tags MUST use the project key.

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
# Sign the tag with the project key (NOT plain `git tag`).
git tag -s v0.1.0 -m "v0.1.0"

# Verify the signature locally before pushing.
git tag -v v0.1.0
# expected: "Good signature from prometheus-remote-writer release signing ..."

git push origin main
git push origin v0.1.0
```

Pushing the tag triggers `.github/workflows/release.yml`:

- Resolves the tag and version.
- Imports the project signing key from secrets.
- Verifies the pushed tag's GPG signature; **fails the workflow if the tag is unsigned**.
- Builds the KAR via `make kar`; generates the SBOM via `make sbom`.
- Extracts the `## [0.1.0]` section from `CHANGELOG.md` as the release body.
- Signs the KAR, the SHA-512 checksum, and the SBOM with the project key.
- Creates a GitHub Release named `v0.1.0` with the KAR + signatures + checksum + SBOM + KEYS attached as assets.

Watch the run:

```bash
gh run watch --repo opennms-forge/prometheus-remote-writer
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
4. **GPG-sign** tag `v0.1.1` on the hotfix branch with the project key:
   `git tag -s v0.1.1 -m "v0.1.1"` and verify with `git tag -v v0.1.1`.
5. Push the tag.

## What gets published

| Artifact | Where | How consumed |
|---|---|---|
| `prometheus-remote-writer-kar-<version>.kar` | GitHub Release asset | Download and install via Karaf `kar:install <path>`. |
| `prometheus-remote-writer-kar-<version>.kar.asc` | GitHub Release asset | Detached GPG signature on the KAR. Verify with `gpg --verify <file>.asc`. |
| `prometheus-remote-writer-kar-<version>.kar.sha512` | GitHub Release asset | SHA-512 checksum of the KAR (GNU coreutils format). Verify integrity with `sha512sum -c <file>.sha512`. |
| `prometheus-remote-writer-kar-<version>.kar.sha512.asc` | GitHub Release asset | Detached GPG signature on the checksum file. Verify with `gpg --verify <file>.sha512.asc`. |
| `prometheus-remote-writer-<version>.cdx.json` | GitHub Release asset | CycloneDX 1.6 SBOM (aggregate across the full Maven reactor); fed to Trivy / Grype / Dependency-Track / FOSSA-style consumers. Generate locally with `make sbom`. |
| `prometheus-remote-writer-<version>.cdx.json.asc` | GitHub Release asset | Detached GPG signature on the SBOM. |
| `KEYS` | GitHub Release asset | ASCII-armored public key of the project signing key. Cross-check the imported key's fingerprint against "Release signing key" above before trusting it. |
| `prometheus-remote-writer-<version>.jar` (bundle) | Not auto-published in v0.1 | Planned. The repo's migration to `opennms-forge` is done; remaining decision is the Maven-repo target (Central via Sonatype, GitHub Packages, or a private Nexus). The same project signing key is reusable for Maven Central PGP requirements. |
| `prometheus-remote-writer-features-<version>-features.xml` | Not auto-published in v0.1 | Same as above — consumed via `feature:repo-add mvn:…/xml/features` when a Maven repo is available. |

## Verifying a release

The signed assets give a consumer two layers of verification: integrity
(via the SHA-512 checksum) and authenticity (via the GPG signature).
For full trust, pair both with the canonical fingerprint cross-check
documented in "Release signing key".

```bash
# 1. Import the project public key (one-time per workstation).
curl -sSL https://github.com/opennms-forge/prometheus-remote-writer/releases/latest/download/KEYS \
  | gpg --import

# 2. Cross-check the imported key's fingerprint against the canonical one
#    in "Release signing key" above. This is the trust anchor.
gpg --fingerprint <KEY_ID>
# expected: matches the fingerprint listed in this README

# 3. Download the artifacts you want to verify.
TAG=v0.1.0
BASE=https://github.com/opennms-forge/prometheus-remote-writer/releases/download/${TAG}
curl -O ${BASE}/prometheus-remote-writer-kar-${TAG#v}.kar
curl -O ${BASE}/prometheus-remote-writer-kar-${TAG#v}.kar.asc
curl -O ${BASE}/prometheus-remote-writer-kar-${TAG#v}.kar.sha512
curl -O ${BASE}/prometheus-remote-writer-kar-${TAG#v}.kar.sha512.asc

# 4. Verify the KAR signature (authenticity).
gpg --verify prometheus-remote-writer-kar-${TAG#v}.kar.asc

# 5. Verify the checksum signature, then the checksum itself (integrity).
gpg --verify prometheus-remote-writer-kar-${TAG#v}.kar.sha512.asc
sha512sum -c prometheus-remote-writer-kar-${TAG#v}.kar.sha512

# 6. (Optional) Verify the SBOM the same way.
curl -O ${BASE}/prometheus-remote-writer-${TAG#v}.cdx.json
curl -O ${BASE}/prometheus-remote-writer-${TAG#v}.cdx.json.asc
gpg --verify prometheus-remote-writer-${TAG#v}.cdx.json.asc
```

**Honest trust note:** A consumer who downloads `KAR + KAR.asc + KEYS`
from the same GitHub Release page is implicitly trusting GitHub. For
strong trust, cross-check the fingerprint against the one published in
this README on a path the consumer trusts independently (the README itself
also lives on GitHub, so the strongest path is to confirm the fingerprint
out of band — e.g. via a maintainer-curated personal site, a public
keyserver, or a printed copy). Documented honestly so the trust model is
clear; the signatures are still meaningful even with weak trust.

## Docs site (GitHub Pages)

A separate workflow,
[`.github/workflows/publish-docs.yml`](.github/workflows/publish-docs.yml),
publishes the rendered single-page HTML documentation to
<https://opennms-forge.github.io/prometheus-remote-writer/> whenever a GitHub
Release is published. The site always reflects the most recent release
tag — older versions are not retained as separate URLs in this release line.

### One-time repo setup

Before the first publish workflow run, enable Pages in the repository:

- **Settings → Pages → Build and deployment → Source: GitHub Actions.**

The workflow needs `pages: write` and `id-token: write`, which are already
declared at workflow scope. No `gh-pages` branch is used.

### Manual republish

If a release ships with a docs typo, fix it on `main` and re-run the
workflow against the same tag — the published site updates without
cutting a new release:

```bash
gh workflow run publish-docs.yml -f tag=v0.3.0
```

The `release: published` trigger fires once per release; `workflow_dispatch`
is for these out-of-band republishes.

## Deferred to a later release

- **Maven artifact publication** — required for the Karaf
  `feature:repo-add mvn:…` install flow shown in the README. The repo
  now lives under the `opennms-forge` namespace; remaining decision is
  the Maven-repo target (Central via Sonatype, GitHub Packages, or a
  private Nexus). The same project signing key documented in
  "Release signing key" satisfies Maven Central's PGP requirement —
  no additional key infrastructure needed.
- **Provenance attestation** — SLSA-style build provenance is a v0.2+ goal.
  Pairs well with the existing GPG signing as a parallel modern-verifier
  path; could be layered via `actions/attest-build-provenance` and
  `actions/attest-sbom` without disturbing the GPG flow.
