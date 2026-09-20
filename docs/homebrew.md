# Homebrew installation and maintenance

## Availability

The public [LambdaDB tap](https://github.com/lambdadb/homebrew-tap) provides stable
`v0.1.6` using the existing release binaries. The tap PR merged on 2026-09-20;
public installation was verified on macOS arm64 and Linux amd64. macOS amd64
and Linux arm64 archives are included and checksum/header-verified, but have
not yet been executed in our installation checks. See the evidence below.

## Consumer commands

With Homebrew installed:

```sh
brew install lambdadb/tap/lambdadb-migration
lambdadb-migration --version
lambdadb-migration --help
lambdadb-migration qdrant --help
lambdadb-migration pinecone --help
lambdadb-migration elasticsearch --help
```

Update an existing installation:

```sh
brew update
brew upgrade lambdadb/tap/lambdadb-migration
```

Uninstall:

```sh
brew uninstall lambdadb/tap/lambdadb-migration
```

Homebrew adds the tap automatically. The formula installs a versioned GitHub
Release binary with a pinned SHA-256 and does not require a separate Go or Node
installation. Follow any formula-specific Homebrew trust prompt. Do not disable
trust checks or change macOS security settings to make installation pass.
The tap can lag the latest stable release while its manual update PR is reviewed.
Uninstalling the package leaves user mappings, checkpoints and credentials intact.
Only untap `lambdadb/tap` if no other tools from that tap are needed.

## Switching installation methods

Before switching, inspect all executable locations:

```sh
type -a lambdadb-migration
ls -l "$(command -v lambdadb-migration)"
brew list --versions lambdadb-migration
```

The standalone installer's default destination is `/usr/local/bin`; its
`--install-dir` option may have selected another directory. Homebrew commonly
uses `/opt/homebrew`, `/usr/local` or `/home/linuxbrew/.linuxbrew`; ask
`brew --prefix` rather than assuming one. A standalone binary can shadow the
Homebrew binary on PATH or conflict with linking in Homebrew's bin directory.

Back up or remove only the standalone executable you have identified before
installing with Homebrew. Do not force linking with `brew link --overwrite`.
After installation, verify `command -v lambdadb-migration` resolves to
`$(brew --prefix)/bin/lambdadb-migration` and check `--version`. If needed, refresh
your shell's command cache with `hash -r`; inspect PATH without rewriting shell
startup files automatically. Preserve mapping files, checkpoints and credentials.

To return to standalone installation, run `brew uninstall
lambdadb/tap/lambdadb-migration` first, then run `install.sh` with an explicit
destination. Do not use `install.sh` to update or uninstall a Homebrew symlink:
the installer can replace or delete the executable at its selected destination.
Use `brew upgrade`/`brew uninstall` for Homebrew-managed installations.

## Maintainer handoff

The tap owns the formula, tests and checksum verifier; do not keep a second
formula copy here. See its
[migration maintenance guide](https://github.com/lambdadb/homebrew-tap/blob/main/MIGRATION.md).

1. Publish and verify a stable migration release using the existing GoReleaser
   process. Confirm macOS/Linux amd64/arm64 archives and `checksums.txt`.
2. Create a tap feature branch from `main`. Update the formula's version, all four
   explicit version URLs and SHA-256 values. Never modify published artifacts or
   tags; packaging-only fixes use a reviewed formula revision where appropriate.
3. Run `python3 scripts/verify-migration-release.py` in the tap checkout. It
   downloads all four archives and compares them against the release checksums,
   formula pins and available GitHub asset digests, and checks binary headers.
4. Run `bash scripts/test-install.sh local lambdadb-migration` and the existing
   CLI test `bash scripts/test-install.sh local`. Pass both macOS/Linux CI jobs
   and review the tap PR. Record which architectures actually ran versus those
   checked only for archive integrity. Tests make no real migration/service writes.
5. After the authorized tap merge, run the `remote` installation checks against
   a checkout matching public `main`. For later releases, also verify an upgrade
   from the prior installed version in a disposable consumer. Record the result.

Initial updates are reviewed manual PRs. The migration runtime, `.goreleaser.yml`
and release workflow stay unchanged; no cross-repository write token is needed.
[GoReleaser's `brews` generator is deprecated](https://goreleaser.com/resources/deprecations/).
The tap deliberately maintains its own formula and `brew test` contract; current
casks can support Linux too. See the tap guide for this choice and the separate
`homebrew/core` acceptance rules. Do not copy old `brews` automation or add
quarantine-removal hooks.

## Public installation verification

On 2026-09-20, tap [PR #1](https://github.com/lambdadb/homebrew-tap/pull/1) merged
as `f2f2f43c2b6ce9f7197e537428a509a67be75971` with the reviewed formula unchanged.

- Local macOS arm64 / Homebrew 6.0.17: public-tap installation, exact formula
  equality, style, version, root and all source/inventory help, offline invalid-URL
  rejection and execution without Go/Node passed. The exact consumer command
  above was also exercised in a separate fresh installation.
- macOS arm64 and Linux amd64: the [post-merge main workflow](https://github.com/lambdadb/homebrew-tap/actions/runs/35492626489)
  passed public `remote` installation of both migration and the existing CLI
  (attempt 2).
- The initial macOS CI attempt hit GitHub's unauthenticated metadata API rate
  limit before installation; the failed job passed on retry without a formula
  change. A tap follow-up addresses verifier authentication to prevent recurrence.
- The installed version was `0.1.6 (7256838f000e7058a5264e83723f83fd4e575f88)`.
  Test installations/taps were removed; existing packages, taps, user files and
  `.env.local` were preserved. No service writes or security bypasses were used.
- All four released archives passed checksum and architecture-header checks.
  macOS amd64/Linux arm64 execution and upgrades between distinct versions remain
  unverified. No new migration release was published for Homebrew registration.
