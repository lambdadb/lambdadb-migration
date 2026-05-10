# LambdaDB Migration

CLI tooling for migrating vector databases and search systems into LambdaDB.

The initial implementation targets Qdrant as the first source, then expands toward source parity with Qdrant's migration tool. LambdaDB is the only target.

See [DESIGN.md](DESIGN.md) for the current architecture and implementation plan.

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

The real smoke suite creates temporary LambdaDB collections, verifies migrated documents with strongly consistent fetches, and deletes the collections in cleanup. It currently covers unnamed dense upsert, named dense upsert, dense+sparse payload-index upsert, and unnamed dense bulk write mode.

Add `--migration.validate` to CLI runs when you want a post-migration check. It compares accepted records against source inventory count, reports LambdaDB `numDocs`, and fetches sample documents by ID to compare migrated fields.

Migration progress is written to stderr with accepted count, percent, batch size, rate, and elapsed time.
