# LambdaDB Migration Handoff

Last updated: 2026-05-11

This document records the current implementation state so work can continue in another chat/session without rediscovering context.

## Project Location

```text
/Users/steven/Dev/lambdadb-migration
```

The folder is now a git repository. Recent implementation commits:

```bash
5762535 Initial LambdaDB migration scaffold
d087187 Fix checkpoint cursors and split write batches
43f9d1c Update handoff after batch work
ca7906c Add migration mapping validation
a807423 Normalize LambdaDB field names
41be3d6 Support YAML mapping files
98e423e Add Qdrant integration fixture
196d62c Handle Qdrant legacy dense vector output
ec30b5a Update handoff after live integration test
279638c Harden Qdrant to LambdaDB migration
```

## Product Direction

This is a migration-to-LambdaDB CLI.

- Sources: eventually support the same source set as Qdrant's migration project.
- Target: LambdaDB only.
- Source implementations: Qdrant is hardened/published; Pinecone Serverless MVP is now implemented.
- Official LambdaDB Go SDK: `github.com/lambdadb/go-lambdadb`.
- Qdrant source client: `github.com/qdrant/go-client/qdrant`.
- Pinecone source client: `github.com/pinecone-io/go-pinecone/v5/pinecone`.

Supported source roadmap from the design:

1. Qdrant
2. Pinecone
3. Chroma
4. Weaviate
5. Elasticsearch/OpenSearch
6. pgvector
7. Milvus/Zilliz
8. Redis
9. MongoDB
10. S3 Vectors
11. FAISS
12. Apache Solr

The architecture intentionally does not fork Qdrant's repository as-is. It uses Qdrant's project as a reference, while keeping LambdaDB target behavior native.

## Important Design Decisions

- Qdrant point IDs are migrated to LambdaDB document `id` by default.
- Qdrant numeric IDs are converted to strings, e.g. `123` -> `"123"`.
- Qdrant UUID IDs remain the same string value.
- `_source_id` is optional and only used when the mapping asks to copy the original ID for audit/debugging or custom ID strategies.
- Unnamed Qdrant dense vector defaults to LambdaDB field `dense`.
- Qdrant sparse vectors are converted from `indices`/`values` arrays into LambdaDB sparse vector objects with string keys.
- Qdrant Manhattan distance is detected but rejected for LambdaDB collection creation because there is no direct LambdaDB similarity equivalent.
- Qdrant multi-vectors are detected and treated as requiring custom migration handling.
- Pinecone Serverless migration uses Pinecone's vector listing API, then fetches vector values and metadata by ID.
- Pinecone dense vectors map to LambdaDB `dense`; Pinecone sparse values map to LambdaDB sparse vector field `sparse`.
- Pinecone metadata index settings are not currently introspected, so generated mappings store metadata payloads without generated LambdaDB index configs.
- Local file checkpoints are the default. The tool does not create a LambdaDB checkpoint collection.
- Bulk upsert is the default LambdaDB write mode, with regular upsert available via flag.

## Current File Structure

```text
.
├── .dockerignore
├── .env.example
├── .goreleaser.yml
├── Dockerfile
├── DESIGN.md
├── LICENSE
├── NOTICE
├── README.md
├── cmd/
│   ├── inventory.go
│   ├── migrate_from_pinecone.go
│   ├── migrate_from_qdrant.go
│   ├── migrate_from_qdrant_test.go
│   ├── output.go
│   ├── output_test.go
│   ├── progress.go
│   ├── progress_test.go
│   └── root.go
├── docs/
│   └── HANDOFF.md
├── go.mod
├── go.sum
├── install.sh
├── integration_tests/
│   ├── compose/
│   │   └── qdrant.yaml
│   ├── pinecone_to_lambdadb_real_test.go
│   ├── qdrant_to_lambdadb_real_test.go
│   └── qdrant_to_lambdadb_test.go
├── internal/
│   ├── checkpoint/
│   │   ├── file_store.go
│   │   ├── file_store_test.go
│   │   └── store.go
│   ├── config/
│   │   ├── field_names.go
│   │   ├── from_inventory.go
│   │   ├── from_inventory_test.go
│   │   ├── mapping.go
│   │   ├── mapping_io.go
│   │   ├── mapping_io_test.go
│   │   ├── mapping_validation.go
│   │   ├── mapping_validation_test.go
│   │   ├── migration.go
│   │   ├── migration_test.go
│   │   ├── pinecone.go
│   │   ├── source.go
│   │   └── target.go
│   ├── pipeline/
│   │   └── runner.go
│   ├── source/
│   │   ├── pinecone/
│   │   │   ├── client.go
│   │   │   ├── client_test.go
│   │   │   ├── inventory.go
│   │   │   └── inventory_test.go
│   │   ├── qdrant/
│   │   │   ├── client.go
│   │   │   ├── cursor_test.go
│   │   │   ├── inventory.go
│   │   │   ├── record.go
│   │   │   └── record_test.go
│   │   └── source.go
│   ├── target/
│   │   └── lambdadb/
│   │       ├── batch.go
│   │       ├── batch_test.go
│   │       ├── client.go
│   │       ├── client_test.go
│   │       ├── schema.go
│   │       └── schema_test.go
│   └── transform/
│       ├── ids.go
│       └── ids_test.go
└── main.go
```

## What Works Now

### CLI Skeleton

The CLI builds and exposes:

```bash
go run . --help
go run . inventory qdrant --help
go run . inventory pinecone --help
go run . qdrant --help
go run . pinecone --help
```

Commands:

- `inventory qdrant`: connects to Qdrant, inspects collection metadata/count, and emits JSON/YAML containing inventory plus generated LambdaDB mapping.
- `inventory pinecone`: connects to Pinecone, inspects Serverless index metadata/count, and emits JSON/YAML containing inventory plus generated LambdaDB mapping.
- `qdrant`: connects to Qdrant and LambdaDB, optionally creates LambdaDB collection, scrolls Qdrant points, transforms to LambdaDB documents, writes to LambdaDB, and saves a local checkpoint.
- `pinecone`: connects to Pinecone and LambdaDB, optionally creates LambdaDB collection, lists/fetches Pinecone vectors, transforms to LambdaDB documents, writes to LambdaDB, and saves a local checkpoint.

### Config

Implemented in `internal/config`:

- `QdrantConfig`
- `PineconeConfig`
- `LambdaDBConfig`
- `MigrationConfig`
- `MappingConfig`
- `DecodeMapping`
- `MappingFromInventory`
- `ValidateMapping`
- `NormalizeFieldName`

`ValidateMapping` currently checks:

- target collection matches the CLI target collection
- dense and sparse vector mappings exist for inventory fields
- vector dimensions are positive, match source dimensions, and stay within LambdaDB's 4,096 dimension limit
- vector similarities are supported before LambdaDB collection creation
- payload index types and text analyzers are supported
- target field names in the mapping do not contain dots
- id, vector, sparse-vector, payload, and payload-index target fields do not collide

Field name behavior:

- default normalization replaces `.` with `_`
- generated mappings normalize vector, sparse-vector, and indexed payload field targets
- generated mappings add `payload.rename` entries for dotted indexed payload fields
- Qdrant inventory warnings suggest normalized names and flag indexed payload collisions
- transform applies the same payload normalization at write time and rejects runtime field collisions

Important CLI flags:

```text
--qdrant.url
--qdrant.api-key
--qdrant.collection
--qdrant.max-message-size
--pinecone.api-key
--pinecone.host
--pinecone.index
--pinecone.namespace
--pinecone.list-prefix
--lambdadb.base-url
--lambdadb.project-name
--lambdadb.api-key
--lambdadb.collection
--migration.batch-size
--migration.max-batch-bytes
--migration.write-mode=bulk|upsert
--migration.restart
--migration.create-collection
--migration.dry-run
--migration.validate
--migration.validation-sample-size
--migration.validation-report
--migration.query-overlap
--migration.query-overlap-limit
--migration.query-overlap-min-ratio
--migration.checkpoint-path
--migration.cleanup-checkpoint
--migration.batch-delay-ms
--migration.retry-max-attempts
--migration.retry-initial-delay-ms
--migration.retry-max-delay-ms
--mapping-file
```

`--migration.validate` now performs post-migration validation:

- checks accepted record count against source inventory count
- reads LambdaDB `numDocs` and reports it
- fetches up to `--migration.validation-sample-size` migrated sample documents with strongly consistent reads
- compares sampled fields, including dense vectors and sparse vectors
- writes a structured JSON report when `--migration.validation-report` is set; the report includes status, counts, sampled IDs, compared count, and errors
- optionally compares source and LambdaDB dense-vector query results for validation samples when `--migration.query-overlap` is set

Note: real LambdaDB smoke tests observed `numDocs=0` even after accepted writes, so `numDocs` is currently reported but not treated as the primary pass/fail signal. Sample fetch/field comparison is the stronger validation check.

Migration progress output now includes accepted count, percent, batch size, rate, and elapsed time, for example:

```text
progress accepted=2/2 (100.0%) batch=2 rate=22.5 records/s elapsed=88ms
migration complete accepted=2 target="articles" rate=22.5 records/s elapsed=88ms
```

### Qdrant Source

Implemented in `internal/source/qdrant`:

- `New(config.QdrantConfig)`
- `Close`
- `Name`
- `Count`
- `Inventory`
- `Read`

Inventory currently extracts:

- record count
- dense vector fields
- named vector fields
- sparse vector field names
- Qdrant payload schema / payload indexes
- warnings for multi-vector and Manhattan distance

Read currently:

- uses Qdrant scroll API
- requests payload and vectors
- converts Qdrant points into neutral `source.Record`
- stores next Qdrant scroll offset as checkpoint cursor

Record conversion currently handles:

- numeric and UUID point IDs to strings
- payload values including scalar, struct, and list values
- dense vectors
- sparse vectors
- multi-vectors as `[][]float32` in neutral records, though later transform rejects them by default
- legacy Qdrant vector outputs that encode dense, sparse, and multi-vector values through deprecated `data` fields

### Pinecone Source

Implemented in `internal/source/pinecone`:

- `New(config.PineconeConfig)`
- `Close`
- `Name`
- `Count`
- `Inventory`
- `Read`
- `SearchDense`

Inventory currently extracts:

- Serverless index count for the configured namespace when available
- dense vector dimension and similarity from `DescribeIndex`
- sparse vector source field for sparse indexes
- warnings for namespace/prefix-scoped migrations and integrated embedding indexes
- warning that Pinecone metadata index settings are not currently introspected

Read currently:

- uses Pinecone `ListVectors`, which is available for Serverless indexes
- fetches listed vector IDs with `FetchVectors`
- converts Pinecone metadata to neutral payload fields
- converts dense values to the unnamed dense vector field
- converts sparse values to source sparse vector field `sparse`
- stores Pinecone pagination tokens as checkpoint cursors

### LambdaDB Target

Implemented in `internal/target/lambdadb`:

- constructs official LambdaDB Go SDK client
- `EnsureCollection`
- `Write`
- `Count`
- converts migration mapping to LambdaDB `indexConfigs`
- retries transient LambdaDB write failures with bounded exponential backoff

`Write` supports:

- bulk mode: `Collection(...).Docs().BulkUpsertDocuments`
- upsert mode: `Collection(...).Docs().Upsert`
- migration writes are split by serialized `{"docs":[...]}` JSON byte size before calling LambdaDB
- regular upsert is capped at 6 MB per request and bulk upsert at 200 MB per request
- write retries currently cover HTTP 429, HTTP 5xx, LambdaDB internal-server errors, network timeouts, connection resets/refusals, and temporary bulk upload failures
- retry attempts and delays are configurable with `--migration.retry-max-attempts`, `--migration.retry-initial-delay-ms`, and `--migration.retry-max-delay-ms`

`EnsureCollection`:

- checks whether the target collection already exists
- creates collection if not found and mapping asks to create one
- waits for the target collection to become `ACTIVE` before writing
- builds vector, sparse vector, scalar, text, and object index configs
- rejects Manhattan/unsupported vector similarity mappings

### Transform

Implemented in `internal/transform`:

- `RecordToDocument`
- `RecordToDocumentWithMapping`

Behavior:

- default document ID is source record ID
- optional source ID copy with `mapping.ids.copyOriginalTo`
- payload field renames via `mapping.payload.rename`
- unnamed dense vector defaults to `dense`
- mapping controls target vector/sparse-vector field names
- multi-vector rejects with a clear error

### Checkpoint

Implemented in `internal/checkpoint`:

- `Store` interface
- `FileStore`
- JSON checkpoint save/load/delete
- checkpoint loads use `json.Decoder.UseNumber` so legacy numeric cursor JSON does not lose uint64 precision
- Qdrant numeric scroll cursors are saved as decimal strings
- checkpoints can be deleted after a successful migration with `--migration.cleanup-checkpoint`

Default checkpoint directory:

```text
.lambdadb-migration/checkpoints
```

Checkpoint keys currently include the source kind, source index/collection, target project, and target collection:

```text
<sourceKind>/<sourceCollection>/<targetProject>/<targetCollection>.json
```

## Verified Commands

These passed as of this handoff:

```bash
cd /Users/steven/Dev/lambdadb-migration
go test ./...
go run . --help
go run . inventory qdrant --help
go run . inventory pinecone --help
go run . qdrant --help
go run . pinecone --help
```

Latest `go test ./...` result:

```text
?    github.com/lambdadb/lambdadb-migration [no test files]
ok   github.com/lambdadb/lambdadb-migration/cmd
ok   github.com/lambdadb/lambdadb-migration/integration_tests
ok   github.com/lambdadb/lambdadb-migration/internal/checkpoint
ok   github.com/lambdadb/lambdadb-migration/internal/config
?    github.com/lambdadb/lambdadb-migration/internal/pipeline [no test files]
?    github.com/lambdadb/lambdadb-migration/internal/source [no test files]
ok   github.com/lambdadb/lambdadb-migration/internal/source/qdrant
ok   github.com/lambdadb/lambdadb-migration/internal/target/lambdadb
ok   github.com/lambdadb/lambdadb-migration/internal/transform
```

Latest gated Qdrant integration result:

```text
docker compose -f integration_tests/compose/qdrant.yaml up -d
LAMBDADB_MIGRATION_RUN_QDRANT_MOCK_E2E=1 go test ./integration_tests -run TestQdrantToLambdaDBMockIntegration -count=1 -v
PASS
```

Latest real LambdaDB smoke result:

```text
docker compose -f integration_tests/compose/qdrant.yaml up -d
LAMBDADB_MIGRATION_RUN_QDRANT_REAL_E2E=1 \
LAMBDADB_BASE_URL="https://aws-dev.lambdadb.ai" \
LAMBDADB_PROJECT_NAME="steven-test" \
LAMBDADB_PROJECT_API_KEY="$LAMBDADB_PROJECT_API_KEY" \
go test ./integration_tests -run TestQdrantToRealLambdaDBSmoke -count=1 -v
PASS
```

Latest Pinecone-to-real-LambdaDB smoke result:

```text
LAMBDADB_MIGRATION_RUN_PINECONE_REAL_E2E=1 \
PINECONE_API_KEY="$PINECONE_API_KEY" \
LAMBDADB_BASE_URL="$LAMBDADB_BASE_URL" \
LAMBDADB_PROJECT_NAME="$LAMBDADB_PROJECT_NAME" \
LAMBDADB_PROJECT_API_KEY="$LAMBDADB_PROJECT_API_KEY" \
go test ./integration_tests -run TestPineconeToRealLambdaDBSmoke -count=1 -v
PASS
validation fetched and compared 2 sample documents
validation query overlap average=1.000 compared=2 limit=2
```

Notes from the real E2E:

- LambdaDB collection creation is asynchronous. `EnsureCollection` now waits until the collection is `ACTIVE` before writing.
- The smoke test now runs with `--migration.validate`, which verifies migrated docs with strongly consistent `Fetch` by ID instead of relying on `numDocs`; `numDocs` has stayed at 0 even though writes were accepted and fetched successfully.
- The unnamed dense real smoke case also runs `--migration.query-overlap` and passed with average overlap 1.000.
- The real smoke suite now covers unnamed dense upsert, named dense upsert, dense+sparse payload-index upsert, additional payload index types, unnamed dense bulk write mode, and a larger dense bulk fixture.
- Bulk write mode passed but can take longer before strongly consistent fetch sees the documents; latest small bulk case took about 96 seconds, the larger 64-document bulk case took about 53 seconds, and a targeted 250-document larger bulk run passed in about 54 seconds after the fetch-limit test fix.
- The test creates unique target collections with short names and deletes them during cleanup.
- Do not commit LambdaDB credentials. `.env`, `.env.*`, and `.env.local` are ignored by git.

## Example Commands

Inventory a local Qdrant collection:

```bash
go run . inventory qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --output qdrant-inventory.yaml
```

Dry-run a migration:

```bash
go run . qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --migration.dry-run
```

Run migration with generated/default mapping:

```bash
go run . qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --migration.batch-size 500 \
  --migration.write-mode bulk
```

Run migration with explicit mapping file:

```bash
go run . qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file qdrant-inventory.yaml
```

Note: inventory commands write YAML for `.yaml`/`.yml` outputs and JSON otherwise. `--mapping-file` accepts either JSON or YAML, as a direct `MappingConfig` object or the wrapped output produced by an inventory command.

## Known Gaps / Risks

### Real E2E Smoke Tested

A Qdrant-to-real-LambdaDB smoke suite has passed against the configured dev project, including larger-batch bulk coverage and additional payload index type coverage.

### LambdaDB Collection Creation Has Broader Smoke Coverage

`EnsureCollection` builds index configs from mapping and now waits for `ACTIVE` after creation. It has been real-service smoke-tested with unnamed dense, named dense, sparse vector, keyword, long, text, double, datetime, boolean, and object index configs.

### Checkpoint Cleanup Implemented And Integration Tested

Checkpoints are retained by default. `--migration.cleanup-checkpoint` deletes the local checkpoint after a successful migration and validation. `TestQdrantToLambdaDBCheckpointCleanup` covers this through the Qdrant-to-LambdaDB mock integration path.

### Retry/Backoff Is Configurable And Covered For Current Scope

LambdaDB writes now retry transient failures with bounded exponential backoff, configurable from CLI flags.

- retry behavior is unit-tested and exercised against a controlled mock 503 fixture
- collection creation/get calls rely on SDK behavior plus the existing `ACTIVE` wait loop
- a real-service controlled 429/5xx fixture is not currently available; treat that as optional future hardening rather than a release blocker

### Validation Has Fetch-Based Report And Query Overlap

`--migration.validate` now checks accepted count and compares configurable fetched sample documents. `--migration.validation-report` writes a JSON report and implies validation. `--migration.query-overlap` compares dense-vector nearest-neighbor result overlap between the source and LambdaDB for validation samples. It reports overlap by default and only fails validation when `--migration.query-overlap-min-ratio` is above `0` and the average falls below that threshold. Remaining validation gaps:

- `numDocs` is reported but not used as a pass/fail signal because real smoke tests observed it staying at 0 after successful writes and fetches
- sparse-vector overlap is feasible because Qdrant supports sparse query vectors and LambdaDB supports `sparseVector` queries, but it is not implemented yet
- hybrid and filter-heavy overlap are technically feasible for curated fixtures, but not generally inferable from mapping alone; they should use explicit representative query fixtures/config if implemented

### Docker And GoReleaser Snapshot Work

Added `Dockerfile`, `.dockerignore`, `.goreleaser.yml`, and README install/build instructions. `docker build -t lambdadb-migration:dev .`, `docker run --rm lambdadb-migration:dev --help`, and `goreleaser release --snapshot --clean` passed locally. Snapshot artifacts were written under ignored `dist/`.

### Installer Added

`install.sh` installs the matching GitHub Release artifact for Linux/macOS amd64/arm64. It supports `--version`, `--install-dir`, `--repo`, `--no-verify`, `--dry-run`, and `--uninstall`. It verifies `checksums.txt` by default. The script has been syntax-checked, dry-run locally, and full install/help/uninstall tested against release `v0.1.3`.

### CI Added

`.github/workflows/ci.yml` runs `go test ./...`, Docker build plus image smoke test, and GoReleaser snapshot on pull requests and pushes to `main`.

### Release Publishing Added

`.github/workflows/release.yml` publishes GoReleaser artifacts on `v*` tag pushes using `GITHUB_TOKEN`. The repository has `origin` configured at `https://github.com/lambdadb/lambdadb-migration.git`, and release workflows have succeeded through `v0.1.3`.

### Integration Coverage Is Better But Still Small

There is now a gated integration test using local Qdrant plus an in-process LambdaDB mock server:

```bash
docker compose -f integration_tests/compose/qdrant.yaml up -d
LAMBDADB_MIGRATION_RUN_QDRANT_MOCK_E2E=1 go test ./integration_tests -run TestQdrantToLambdaDBMockIntegration -count=1
```

Current fixtures cover:

- unnamed dense vector plus dotted payload field normalization
- named dense vectors
- dense + sparse vectors
- Qdrant payload indexes
- additional payload index types: text, double, datetime, boolean, object
- transient LambdaDB write retry using controlled HTTP 503 responses
- checkpoint cleanup after successful validation
- structured validation report creation
- dense-vector query overlap validation
- Manhattan distance rejection
- multi-vector rejection

This gated integration test passed against the local Docker Qdrant fixture in this workspace. It also caught and fixed real Qdrant scroll responses that return vector data through deprecated `data` fields instead of the newer typed oneofs.

There is also a real Qdrant-to-LambdaDB smoke suite gated by `LAMBDADB_MIGRATION_RUN_QDRANT_REAL_E2E=1`.

There is a Pinecone-to-real-LambdaDB smoke suite gated by `LAMBDADB_MIGRATION_RUN_PINECONE_REAL_E2E=1`. It creates a disposable Pinecone Serverless index using `PINECONE_API_KEY` and optional `LAMBDADB_MIGRATION_PINECONE_CLOUD` / `LAMBDADB_MIGRATION_PINECONE_REGION` overrides, defaulting to `aws` / `us-east-1`, upserts fixture vectors, migrates into a temporary LambdaDB collection, verifies fetched documents, and deletes both resources in cleanup.

Remaining integration risk: controlled failure/retry behavior is still mock-only. The heavier live bulk run has been tested at 250 documents; treat substantially larger customer-scale volumes as optional future confidence testing when API cost/time is acceptable.

## Suggested Next Work Order

Completed for current publish scope:

1. Release workflows have run successfully after remote configuration.
2. Release `v0.1.3` is published and marked latest.
3. Installer full download/install/help/uninstall has been verified against `v0.1.3`.
4. Larger live bulk coverage has reached 250 documents; larger customer-scale runs are optional rather than a near-term blocker.
5. Pinecone Serverless MVP and disposable-index live smoke have passed.

Recommended next work:

1. Review and commit the Pinecone connector, disposable live smoke, env cleanup, and docs updates.
2. If query validation is the priority, implement sparse-vector query overlap first. Hybrid and filter-heavy overlap should wait for explicit representative query fixtures/config.
3. Continue source coverage after Pinecone, likely Chroma or Weaviate depending on customer pull.

## Files To Read First In The Next Session

Start here:

1. `docs/HANDOFF.md`
2. `DESIGN.md`
3. `cmd/migrate_from_qdrant.go`
4. `internal/source/qdrant/client.go`
5. `internal/source/qdrant/inventory.go`
6. `internal/source/qdrant/record.go`
7. `internal/target/lambdadb/client.go`
8. `internal/target/lambdadb/schema.go`
9. `internal/transform/ids.go`

## Current Dependencies

From `go.mod`:

```text
github.com/alecthomas/kong v1.13.0
github.com/lambdadb/go-lambdadb v0.3.0
github.com/pinecone-io/go-pinecone/v5 v5.4.1
github.com/qdrant/go-client v1.17.1
```

There are many indirect dependencies from Qdrant's gRPC/protobuf client, Pinecone's SDK, and LambdaDB SDK.

## Caveat

The Qdrant path is release-published and covered by local/mock plus real LambdaDB smoke tests. The Pinecone path is a first Serverless MVP with unit coverage and CLI/help verification; run a gated live Pinecone smoke before treating it as release-ready.
