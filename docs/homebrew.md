# Homebrew installation and maintenance

## Availability gate

Homebrew support for the existing stable `v0.1.6` is being prepared in the public
[LambdaDB tap](https://github.com/lambdadb/homebrew-tap). It is **not yet verified
as publicly installable**. Merge the tap PR first, verify installation from the
actual public tap on macOS/Linux, then finalize this page and the README before
merging the migration documentation PR. No new migration release is required.

## Consumer commands after publication

With Homebrew installed, macOS/Linux amd64/arm64 consumers will use:

```sh
brew install lambdadb/tap/lambdadb-migration
lambdadb-migration --version
lambdadb-migration --help
lambdadb-migration qdrant --help
lambdadb-migration pinecone --help
lambdadb-migration elasticsearch --help
brew update
brew upgrade lambdadb/tap/lambdadb-migration
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
[migration maintenance guide](https://github.com/lambdadb/homebrew-tap/blob/main/MIGRATION.md)
once the tap PR merges.

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
