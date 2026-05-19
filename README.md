# LambdaDB Migration

CLI tooling for migrating vector databases and search systems into LambdaDB.

Supported sources include Qdrant and Pinecone Serverless. LambdaDB is the only target.

## Quickstart

Install the latest release:

```bash
curl -fsSL https://raw.githubusercontent.com/lambdadb/lambdadb-migration/main/install.sh | sh
```

If `/usr/local/bin` requires elevated permissions, the installer will ask for `sudo`. To install somewhere else:

```bash
curl -fsSLO https://raw.githubusercontent.com/lambdadb/lambdadb-migration/main/install.sh
sh install.sh --install-dir "$HOME/.local/bin"
```

Make sure the install directory is on your `PATH`, then check the CLI:

```bash
lambdadb-migration --help
lambdadb-migration qdrant --help
lambdadb-migration pinecone --help
```

Install a specific version:

```bash
sh install.sh --version v0.1.3 --install-dir "$HOME/.local/bin"
```

Uninstall the binary:

```bash
sh install.sh --uninstall
```

If you installed to a custom directory:

```bash
sh install.sh --uninstall --install-dir "$HOME/.local/bin"
```

Review the installer before running it:

```bash
curl -fsSLO https://raw.githubusercontent.com/lambdadb/lambdadb-migration/main/install.sh
less install.sh
sh install.sh
```

## Qdrant To LambdaDB

Set your LambdaDB connection values from the LambdaDB Cloud console. LambdaDB Cloud uses region-specific API base URLs, so do not assume a global default URL or a fixed project name.

```bash
export LAMBDADB_BASE_URL="your-region-specific-lambdadb-base-url"
export LAMBDADB_PROJECT_NAME="your-lambdadb-project-name"
export LAMBDADB_PROJECT_API_KEY="your-project-api-key"
```

Generate an inventory and editable mapping from Qdrant:

```bash
lambdadb-migration inventory qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --output qdrant-inventory.yaml
```

Review `qdrant-inventory.yaml`, then run a dry-run:

```bash
lambdadb-migration qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file qdrant-inventory.yaml \
  --migration.dry-run
```

Run the migration with validation:

```bash
lambdadb-migration qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file qdrant-inventory.yaml \
  --migration.write-mode bulk \
  --migration.validate \
  --migration.validation-report validation-report.json
```

Migration progress is written to stderr with accepted count, percent, batch size, rate, and elapsed time.

## Pinecone To LambdaDB

Set your Pinecone API key and LambdaDB connection values:

```bash
export PINECONE_API_KEY="your-pinecone-api-key"
export LAMBDADB_BASE_URL="your-region-specific-lambdadb-base-url"
export LAMBDADB_PROJECT_NAME="your-lambdadb-project-name"
export LAMBDADB_PROJECT_API_KEY="your-project-api-key"
```

Generate an inventory and editable mapping from a Pinecone Serverless index:

```bash
lambdadb-migration inventory pinecone \
  --pinecone.index articles \
  --pinecone.namespace production \
  --output pinecone-inventory.yaml
```

Review `pinecone-inventory.yaml`, then run a dry-run:

```bash
lambdadb-migration pinecone \
  --pinecone.index articles \
  --pinecone.namespace production \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file pinecone-inventory.yaml \
  --migration.dry-run
```

Run the migration with validation:

```bash
lambdadb-migration pinecone \
  --pinecone.index articles \
  --pinecone.namespace production \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file pinecone-inventory.yaml \
  --migration.write-mode bulk \
  --migration.validate \
  --migration.validation-report validation-report.json
```

The Pinecone connector uses Pinecone's vector listing API, which is available for Serverless indexes. Use `--pinecone.list-prefix` to migrate only IDs with a specific prefix.

## Common Options

`inventory qdrant` and `inventory pinecone` write YAML for `.yaml`/`.yml` outputs and JSON otherwise. `--mapping-file` accepts either JSON or YAML, as a direct mapping object or as the wrapped output produced by an inventory command.

Useful migration safety flags:

```text
--migration.create-collection=false
--migration.validation-sample-size 10
--migration.validation-report validation-report.json
--migration.query-overlap
--migration.query-overlap-limit 5
--migration.query-overlap-min-ratio 0
--migration.retry-max-attempts 5
--migration.retry-initial-delay-ms 500
--migration.retry-max-delay-ms 5000
--migration.cleanup-checkpoint
```

Generated inventory mappings set `target.createCollection: true` by default, so the migration creates the LambdaDB collection when it is missing. Use `--migration.create-collection=false` to require the target collection to exist, or `--migration.create-collection=true` to override a mapping file that has collection creation disabled.

`--migration.validation-report` writes a JSON report with pass/fail status, source and accepted counts, LambdaDB `numDocs`, sampled document IDs, compared sample count, query overlap results, and validation errors. Setting it also enables validation.

`--migration.query-overlap` adds dense and sparse vector query overlap checks for validation samples when those vector mappings are present. By default it reports overlap without failing; set `--migration.query-overlap-min-ratio` above `0` to require a minimum average overlap.

## Uninstall

The installer only places a single binary on disk. To remove it:

```bash
sh install.sh --uninstall
```

Or remove it manually:

```bash
sudo rm /usr/local/bin/lambdadb-migration
```

If you installed into a user directory:

```bash
rm "$HOME/.local/bin/lambdadb-migration"
```

Local migration checkpoints are stored in the directory where you ran the migration. Remove them separately if you no longer need resume state:

```bash
rm -rf .lambdadb-migration
```

## Mapping Examples

For named vectors, generate an inventory first and review the generated `vectors` mapping:

```yaml
vectors:
  title_dense:
    targetField: title_dense
    dimensions: 384
    similarity: cosine
  body_dense:
    targetField: body_dense
    dimensions: 768
    similarity: dot_product
```

For hybrid-style dense plus sparse data, keep both vector mappings and indexed payload fields explicit:

```yaml
vectors:
  body_dense:
    targetField: body_dense
    dimensions: 768
    similarity: cosine
sparseVectors:
  keywords_sparse:
    targetField: keywords_sparse
payload:
  mode: flatten
  indexConfigs:
    category:
      type: keyword
    views:
      type: long
```

## Development

See [DESIGN.md](DESIGN.md) for the current architecture and implementation plan.

Build a local binary:

```bash
go build -o bin/lambdadb-migration .
bin/lambdadb-migration --help
```

Run from source:

```bash
go run . --help
go run . inventory qdrant --help
go run . inventory pinecone --help
go run . qdrant --help
go run . pinecone --help
```

Build a Docker image:

```bash
docker build \
  --build-arg VERSION=dev \
  --build-arg COMMIT="$(git rev-parse --short HEAD)" \
  -t lambdadb-migration:dev .
```

Run from Docker:

```bash
docker run --rm lambdadb-migration:dev --help
```

Create a local release snapshot with GoReleaser:

```bash
goreleaser release --snapshot --clean
```

Publish a GitHub release:

```bash
git tag v0.1.3
git push origin v0.1.3
```

Tag pushes matching `v*` run the release workflow and publish GoReleaser artifacts to GitHub Releases.

## Integration Tests

Start local Qdrant, then run the gated integration test:

```bash
docker compose -f integration_tests/compose/qdrant.yaml up -d
LAMBDADB_MIGRATION_RUN_QDRANT_MOCK_E2E=1 go test ./integration_tests -run TestQdrantToLambdaDBMockIntegration -count=1
```

The test seeds temporary Qdrant collections and migrates them through the CLI path into an in-process LambdaDB mock server.

For controlled end-to-end checks against a real LambdaDB project, copy `.env.example` to `.env.local`, fill in credentials, and run:

```bash
set -a
source .env.local
set +a

docker compose -f integration_tests/compose/qdrant.yaml up -d
go test ./integration_tests -run TestQdrantToRealLambdaDBSmoke -count=1 -v
```

Set `LAMBDADB_MIGRATION_RUN_QDRANT_REAL_E2E=1` in `.env.local` before running this real Qdrant-to-LambdaDB smoke test.

Local `.env` files are ignored by git. Do not commit real API keys.

The real smoke suite creates temporary LambdaDB collections, verifies migrated documents with strongly consistent fetches, and deletes the collections in cleanup. It currently covers unnamed dense upsert, named dense upsert, dense+sparse payload-index upsert, additional payload index types, unnamed dense bulk write mode, and a larger dense bulk fixture.

For a controlled Pinecone-to-LambdaDB smoke test, set `LAMBDADB_MIGRATION_RUN_PINECONE_REAL_E2E=1` and `PINECONE_API_KEY` in `.env.local`. The test creates its disposable Pinecone index in `aws` / `us-east-1` by default; override that with `LAMBDADB_MIGRATION_PINECONE_CLOUD` and `LAMBDADB_MIGRATION_PINECONE_REGION` when needed. Then run:

```bash
set -a
source .env.local
set +a

go test ./integration_tests -run TestPineconeToRealLambdaDBSmoke -count=1 -v
```

The Pinecone smoke test creates disposable dense and sparse Pinecone Serverless indexes, upserts fixture vectors, migrates them into temporary LambdaDB collections, verifies fetched documents, checks query overlap, and deletes all resources in cleanup.

The larger fixture defaults to 64 records. Override it with:

```bash
LAMBDADB_MIGRATION_QDRANT_REAL_LARGE_COUNT=250
```
