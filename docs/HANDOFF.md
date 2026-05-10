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
│   └── root.go
├── docs/
│   └── HANDOFF.md
├── go.mod
├── go.sum
├── internal/
│   ├── checkpoint/
│   │   ├── file_store.go
│   │   ├── file_store_test.go
│   │   └── store.go
│   ├── config/
│   │   ├── from_inventory.go
│   │   ├── mapping.go
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

- `inventory qdrant`: connects to Qdrant, inspects collection metadata/count, and emits JSON containing inventory plus generated LambdaDB mapping.
- `qdrant`: connects to Qdrant and LambdaDB, optionally creates LambdaDB collection, scrolls Qdrant points, transforms to LambdaDB documents, writes to LambdaDB, and saves a local checkpoint.

### Config

Implemented in `internal/config`:

- `QdrantConfig`
- `LambdaDBConfig`
- `MigrationConfig`
- `MappingConfig`
- `MappingFromInventory`

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
  --output qdrant-inventory.json
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
  --mapping-file qdrant-inventory.json
```

Note: `--mapping-file` currently accepts either a direct `MappingConfig` JSON object or the wrapped JSON produced by `inventory qdrant`.

## Known Gaps / Risks

### Not Yet E2E Tested Against Real Services

The code compiles and unit tests pass, but no Qdrant/LambdaDB end-to-end migration has been run yet in this workspace.

### YAML Mapping Not Implemented

Design mentions JSON/YAML. Current code only reads JSON.

Options:

- add `gopkg.in/yaml.v3`
- or keep V1 JSON-only and update docs/README accordingly

### Field Name Normalization Not Implemented

Design says LambdaDB field names cannot contain dots and should be renamed. Current code only applies explicit `mapping.payload.rename`; it does not automatically normalize dots or detect collisions.

Need to add:

- default dot replacement, probably `.` -> `_`
- collision detection
- clear error messages
- inventory warnings/suggested renames

### Mapping Validation Is Thin

Need validation before writing:

- vector field mappings exist for all source vectors
- dimensions are within LambdaDB limit
- unsupported similarities rejected early
- payload index types are valid
- target collection in mapping matches CLI target collection or override rules are explicit
- managed embeddings and bulk mode incompatibility

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

### No Integration Test Fixture Yet

Need local Qdrant fixture and LambdaDB mock server.

Suggested fixtures:

- single dense vector
- named dense vectors
- dense + sparse
- payload indexes
- field names containing dots
- Manhattan distance
- multi-vector

## Suggested Next Work Order

1. Commit the checkpoint cursor and batch-splitting changes.
2. Add mapping validation before collection creation/writes.
3. Add field-name normalization and collision detection.
4. Add YAML support or explicitly document JSON-only V1.
5. Add Qdrant docker compose fixture and a LambdaDB mock server integration test.
6. Run a real local Qdrant inventory test.
7. Run a controlled LambdaDB test project migration with a tiny dataset.
8. Add progress output that is nicer than plain `accepted x/y`.

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
