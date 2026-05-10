# LambdaDB Migration Handoff

Last updated: 2026-05-10

This document records the current implementation state so work can continue in another chat/session without rediscovering context.

## Project Location

```text
/Users/steven/Dev/lambdadb-migration
```

The folder is now a git repository. Baseline scaffold commit:

```bash
5762535 Initial LambdaDB migration scaffold
d087187 Fix checkpoint cursors and split write batches
43f9d1c Update handoff after batch work
ca7906c Add migration mapping validation
a807423 Normalize LambdaDB field names
41be3d6 Support YAML mapping files
```

## Product Direction

This is a migration-to-LambdaDB CLI.

- Sources: eventually support the same source set as Qdrant's migration project.
- Target: LambdaDB only.
- First source implementation: Qdrant.
- Official LambdaDB Go SDK: `github.com/lambdadb/go-lambdadb`.
- Qdrant source client: `github.com/qdrant/go-client/qdrant`.

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
- Local file checkpoints are the default. The tool does not create a LambdaDB checkpoint collection.
- Bulk upsert is the default LambdaDB write mode, with regular upsert available via flag.

## Current File Structure

```text
.
├── DESIGN.md
├── LICENSE
├── NOTICE
├── README.md
├── cmd/
│   ├── inventory.go
│   ├── migrate_from_qdrant.go
│   ├── output.go
│   ├── output_test.go
│   └── root.go
├── docs/
│   └── HANDOFF.md
├── go.mod
├── go.sum
├── integration_tests/
│   ├── compose/
│   │   └── qdrant.yaml
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
│   │   ├── source.go
│   │   └── target.go
│   ├── pipeline/
│   │   └── runner.go
│   ├── source/
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
go run . qdrant --help
```

Commands:

- `inventory qdrant`: connects to Qdrant, inspects collection metadata/count, and emits JSON/YAML containing inventory plus generated LambdaDB mapping.
- `qdrant`: connects to Qdrant and LambdaDB, optionally creates LambdaDB collection, scrolls Qdrant points, transforms to LambdaDB documents, writes to LambdaDB, and saves a local checkpoint.

### Config

Implemented in `internal/config`:

- `QdrantConfig`
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
--migration.checkpoint-path
--migration.batch-delay-ms
--mapping-file
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

### LambdaDB Target

Implemented in `internal/target/lambdadb`:

- constructs official LambdaDB Go SDK client
- `EnsureCollection`
- `Write`
- `Count`
- converts migration mapping to LambdaDB `indexConfigs`

`Write` supports:

- bulk mode: `Collection(...).Docs().BulkUpsertDocuments`
- upsert mode: `Collection(...).Docs().Upsert`
- migration writes are split by serialized `{"docs":[...]}` JSON byte size before calling LambdaDB
- regular upsert is capped at 6 MB per request and bulk upsert at 200 MB per request

`EnsureCollection`:

- checks whether the target collection already exists
- creates collection if not found and mapping asks to create one
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

Default checkpoint directory:

```text
.lambdadb-migration/checkpoints
```

Checkpoint key currently includes:

```text
qdrant/<sourceCollection>/<targetProject>/<targetCollection>.json
```

## Verified Commands

These passed as of this handoff:

```bash
cd /Users/steven/Dev/lambdadb-migration
go test ./...
go run . --help
go run . inventory qdrant --help
go run . qdrant --help
```

Latest `go test ./...` result:

```text
ok   github.com/lambdadb/lambdadb-migration/cmd
ok   github.com/lambdadb/lambdadb-migration/integration_tests
ok   github.com/lambdadb/lambdadb-migration/internal/checkpoint
ok   github.com/lambdadb/lambdadb-migration/internal/config
ok   github.com/lambdadb/lambdadb-migration/internal/source/qdrant
ok   github.com/lambdadb/lambdadb-migration/internal/target/lambdadb
ok   github.com/lambdadb/lambdadb-migration/internal/transform
```

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
  --lambdadb.project-name playground \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --migration.dry-run
```

Run migration with generated/default mapping:

```bash
go run . qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --lambdadb.project-name playground \
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
  --lambdadb.project-name playground \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file qdrant-inventory.yaml
```

Note: `inventory qdrant` writes YAML for `.yaml`/`.yml` outputs and JSON otherwise. `--mapping-file` accepts either JSON or YAML, as a direct `MappingConfig` object or the wrapped output produced by `inventory qdrant`.

## Known Gaps / Risks

### Not Yet E2E Tested Against Real Services

The code compiles and unit tests pass, but no Qdrant/LambdaDB end-to-end migration has been run yet in this workspace.

### LambdaDB Collection Creation Is Basic

`EnsureCollection` builds index configs from mapping, but has not been verified with real LambdaDB API.

Check especially:

- `components.IndexConfigsObject` for object fields
- sparse vector index config shape
- `ResourceNotFoundError` handling from SDK
- text analyzer JSON shape

### Checkpoint Cleanup Not Implemented

Checkpoints are retained after success. Design mentions cleanup only if user asks. No `--cleanup-checkpoint` flag yet.

### No Retry/Backoff

Current LambdaDB writes call SDK directly. Need retry handling for:

- HTTP 429
- transient network errors
- bulk upload temporary failures

### No Validation Command/Report Yet

`--migration.validate` flag exists but does nothing.

Need:

- source count vs accepted count
- LambdaDB `numDocs` polling after bulk indexing
- fetch sample IDs and compare selected fields
- optional query overlap later

### No Docker / Release Build Yet

Need:

- Dockerfile
- goreleaser or release workflow
- README install instructions

### Integration Coverage Is Still Thin

There is now a gated integration test using local Qdrant plus an in-process LambdaDB mock server:

```bash
docker compose -f integration_tests/compose/qdrant.yaml up -d
LAMBDADB_MIGRATION_RUN_INTEGRATION=1 go test ./integration_tests -run TestQdrantToLambdaDBMockIntegration -count=1
```

Current fixture covers a single dense-vector collection with payload field-name normalization. More fixture coverage is still needed.

Note: `docker compose -f integration_tests/compose/qdrant.yaml up -d` was attempted in this workspace, but the Qdrant image pull stalled and was interrupted. The gated integration test has been compile-verified and defaults to skipped unless `LAMBDADB_MIGRATION_RUN_INTEGRATION=1` is set.

Suggested fixtures:

- single dense vector
- named dense vectors
- dense + sparse
- payload indexes
- field names containing dots
- Manhattan distance
- multi-vector

## Suggested Next Work Order

1. Run a real local Qdrant inventory test.
2. Add integration fixtures for named vectors, dense+sparse, payload indexes, Manhattan, and multi-vector cases.
3. Run a controlled LambdaDB test project migration with a tiny dataset.
4. Add progress output that is nicer than plain `accepted x/y`.

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
github.com/qdrant/go-client v1.17.1
```

There are many indirect dependencies from Qdrant's gRPC/protobuf client and LambdaDB SDK.

## Caveat

The current migration command is intentionally a first thin path, not production-ready. It should be treated as a scaffold that can run after more validation and batch-safety work, rather than as the finished customer-facing tool.
