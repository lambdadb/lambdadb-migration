# LambdaDB Migration

CLI tooling for migrating vector databases and search systems into LambdaDB.

The initial implementation targets Qdrant as the first source, then expands toward source parity with Qdrant's migration tool. LambdaDB is the only target.

See [DESIGN.md](DESIGN.md) for the current architecture and implementation plan.

## Install / Build

Build a local binary:

```bash
go build -o bin/lambdadb-migration .
bin/lambdadb-migration --help
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

## Mapping Files

Generate an inventory and editable mapping from Qdrant:

```bash
go run . inventory qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --output qdrant-inventory.yaml
```

`inventory qdrant` writes YAML for `.yaml`/`.yml` outputs and JSON otherwise. `--mapping-file` accepts either JSON or YAML, as a direct mapping object or as the wrapped output produced by `inventory qdrant`.

## Integration Test Fixture

Start local Qdrant, then run the gated integration test:

```bash
docker compose -f integration_tests/compose/qdrant.yaml up -d
LAMBDADB_MIGRATION_RUN_INTEGRATION=1 go test ./integration_tests -run TestQdrantToLambdaDBMockIntegration -count=1
```

The test seeds temporary Qdrant collections and migrates them through the CLI path into an in-process LambdaDB mock server.

## Real LambdaDB Smoke Test

For controlled end-to-end checks against a real LambdaDB project, start local Qdrant and provide LambdaDB credentials through environment variables:

```bash
docker compose -f integration_tests/compose/qdrant.yaml up -d
LAMBDADB_MIGRATION_RUN_REAL_E2E=1 \
LAMBDADB_BASE_URL="https://..." \
LAMBDADB_PROJECT_NAME="..." \
LAMBDADB_PROJECT_API_KEY="$LAMBDADB_PROJECT_API_KEY" \
go test ./integration_tests -run TestQdrantToRealLambdaDBSmoke -count=1 -v
```

Local `.env` files are ignored by git. Prefer `.env.local` for reusable local credentials, and do not commit real API keys.

The real smoke suite creates temporary LambdaDB collections, verifies migrated documents with strongly consistent fetches, and deletes the collections in cleanup. It currently covers unnamed dense upsert, named dense upsert, dense+sparse payload-index upsert, additional payload index types, unnamed dense bulk write mode, and a larger dense bulk fixture.

The larger fixture defaults to 64 records. Override it with:

```bash
LAMBDADB_MIGRATION_REAL_LARGE_COUNT=250
```

Add `--migration.validate` to CLI runs when you want a post-migration check. It compares accepted records against source inventory count, reports LambdaDB `numDocs`, and fetches sample documents by ID to compare migrated fields.

Useful migration safety flags:

```text
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

`--migration.validation-report` writes a JSON report with pass/fail status, source and accepted counts, LambdaDB `numDocs`, sampled document IDs, compared sample count, and validation errors. Setting it also enables validation.

`--migration.query-overlap` adds dense-vector query overlap checks for validation samples. By default it reports overlap without failing; set `--migration.query-overlap-min-ratio` above `0` to require a minimum average overlap.

Migration progress is written to stderr with accepted count, percent, batch size, rate, and elapsed time.
