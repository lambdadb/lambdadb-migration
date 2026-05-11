# LambdaDB Migration Tool Design

## Purpose

`lambdadb-migration` is a CLI tool that helps teams move data from vector databases and search systems into LambdaDB. The first implementation should focus on Qdrant-to-LambdaDB because LambdaDB already has public migration guidance for Qdrant, Qdrant exposes a reliable scroll API, and Qdrant's open-source migration tool provides a useful reference for connector behavior, resumability, and CLI ergonomics.

This project should be a new LambdaDB-owned repository, not a public GitHub fork of `qdrant/migration`. We can reuse Apache-2.0 licensed code where it makes sense, but the target-side architecture should be LambdaDB-native rather than a Qdrant writer with names changed.

## Reference Inputs

- Qdrant migration tool: https://github.com/qdrant/migration
- Qdrant migration license: Apache License 2.0, copyright notice for Qdrant Solutions GmbH.
- LambdaDB docs index: https://docs.lambdadb.ai/llms.txt
- LambdaDB Qdrant migration guide: https://docs.lambdadb.ai/guides/migrations/migrate-from-qdrant.md
- LambdaDB bulk upsert docs: https://docs.lambdadb.ai/guides/documents/bulk-upsert-data
- LambdaDB Go SDK: `github.com/lambdadb/go-lambdadb`

## Product Goals

1. Make migration from a competitor feel low-risk: dry-run, inventory, schema preview, resumable writes, validation, and clear error messages.
2. Support source parity with Qdrant's migration project over time: Chroma, Pinecone, Milvus, Weaviate, Redis, MongoDB, OpenSearch, Elasticsearch, Postgres/pgvector, S3 Vectors, FAISS, Apache Solr, and Qdrant.
3. Keep LambdaDB as the only target. This project is a migration-to-LambdaDB tool, not a general-purpose source-to-target ETL framework.
4. Preserve user-visible retrieval behavior where possible: IDs, payload fields, vector fields, sparse vectors, indexed filter fields, and hybrid-search fields.
5. Prefer LambdaDB-native bulk ingestion for large unmanaged-vector imports.
6. Avoid hiding hard migration decisions: multi-vectors, unsupported metrics, field-name collisions, large documents, and managed embedding tradeoffs should be surfaced explicitly.
7. Keep the first version small enough to ship and support.

## Non-Goals For V1

- Live CDC or continuous replication.
- Automatic query rewriting for every source SDK.
- Fully automatic schema inference for every payload field.
- Qdrant multi-vector or ColBERT matrix migration without explicit user configuration.
- Cross-provider relevance parity guarantees.
- Migrating from LambdaDB into other databases.
- Supporting non-LambdaDB targets.

## Why Qdrant's Project Is The Best Starting Reference

Qdrant's `migration` project is a Go CLI with source-specific commands such as `qdrant`, `pinecone`, `chroma`, `weaviate`, `elasticsearch`, `opensearch`, `pg`, `redis`, `mongodb`, `s3`, `faiss`, and `solr`. Its useful parts are:

- CLI command shape via `kong`.
- Source connection config structs.
- Source-specific pagination logic.
- Qdrant scroll/read patterns.
- Retry/backoff for transient write errors.
- Progress UI.
- Integration-test strategy with local services.

The parts that should not be copied directly as the core architecture:

- Target-side code assumes Qdrant collections, points, gRPC, payload indexes, shard keys, and Qdrant offset collections.
- Resumability is stored in a Qdrant collection. LambdaDB migrations should default to a local checkpoint file to avoid polluting the customer's LambdaDB project, with optional LambdaDB-backed checkpointing later.
- Upsert logic writes Qdrant `PointStruct`. LambdaDB needs JSON documents and should prefer `BulkUpsertDocuments` for large batches.

## Proposed Repository Layout

```text
lambdadb-migration/
  DESIGN.md
  README.md
  LICENSE
  NOTICE
  go.mod
  go.sum
  main.go
  cmd/
    root.go
    inventory.go
    migrate_from_qdrant.go
    migrate_from_pinecone.go
    migrate_from_chroma.go
  internal/
    cli/
      output.go
      errors.go
      flags.go
    config/
      migration.go
      target_lambdadb.go
      sources.go
      mapping.go
    source/
      source.go
      qdrant/
        client.go
        inventory.go
        reader.go
        mapper.go
      pinecone/
      chroma/
      weaviate/
    target/
      lambdadb/
        client.go
        collection.go
        writer.go
        bulk_writer.go
        count.go
    transform/
      record.go
      schema.go
      field_names.go
      sparse.go
      ids.go
    checkpoint/
      store.go
      file_store.go
      lambdadb_store.go
    pipeline/
      runner.go
      batcher.go
      retry.go
      progress.go
    verify/
      counts.go
      samples.go
      search_eval.go
    license/
      attribution.go
  examples/
    qdrant-basic.yaml
    qdrant-hybrid.yaml
  integration_tests/
    compose/
      qdrant.yaml
    qdrant_to_lambdadb_test.go
```

Use `internal/` for most code at first. A stable public Go library API can come later if users ask to embed the migration engine.

## Core Interfaces

```go
type Source interface {
    Name() string
    Inventory(ctx context.Context) (*Inventory, error)
    Count(ctx context.Context) (uint64, error)
    Read(ctx context.Context, cursor Cursor, limit int) (Batch, error)
}

type Target interface {
    EnsureCollection(ctx context.Context, inv *Inventory, mapping MappingConfig) error
    Write(ctx context.Context, docs []map[string]any) error
    Count(ctx context.Context) (uint64, error)
}

type CheckpointStore interface {
    Load(ctx context.Context, key string) (*Checkpoint, error)
    Save(ctx context.Context, key string, checkpoint Checkpoint) error
    Delete(ctx context.Context, key string) error
}
```

Source adapters should return a neutral `Record`, not LambdaDB documents directly:

```go
type Record struct {
    ID      string
    Payload map[string]any
    Vectors map[string]VectorValue
}

type VectorValue struct {
    Dense  []float32
    Sparse map[string]float32
    Multi  [][]float32
}
```

The `transform` package converts `Record` into a LambdaDB document after applying field rename, vector mapping, sparse conversion, ID policy, and payload nesting rules.

## LambdaDB Target Design

The target client should use the official LambdaDB Go SDK package:

```bash
go get github.com/lambdadb/go-lambdadb
```

```go
import lambdadb "github.com/lambdadb/go-lambdadb"

client := lambdadb.New(
    lambdadb.WithBaseURL(cfg.BaseURL),
    lambdadb.WithProjectName(cfg.ProjectName),
    lambdadb.WithAPIKey(cfg.APIKey),
)
```

The writer should support two modes:

- `bulk`: default for unmanaged vector migrations. Use `Collection(name).Docs().BulkUpsertDocuments(ctx, lambdadb.UpsertDocsInput{Docs: docs})`. Respect the API-provided 200 MB object limit and split batches by JSON byte size.
- `upsert`: required for collections with managed embedding fields and useful for small batches. Respect the regular 6 MB request payload limit.

Important LambdaDB constraints from docs and SDK:

- Upsert payload limit: 6 MB.
- Bulk upsert object limit: 200 MB.
- Max document size: 5 MB.
- Max vector dimensions: 4,096.
- Document IDs are strings and max length is 512.
- Bulk upsert is asynchronous, so validation must poll or wait before expecting `numDocs` to match.
- Bulk upsert is not supported for collections with managed embedding vector fields.

## Qdrant To LambdaDB Mapping

| Qdrant | LambdaDB | Implementation |
| --- | --- | --- |
| Collection | Collection | One target collection per source collection by default. |
| Point ID | Document `id` | Convert numeric and UUID IDs to strings. |
| Unnamed dense vector | `dense` or configured field | Default field `dense`, user configurable. |
| Named dense vector | Vector field with same name | Validate dimensions and metric. |
| Sparse vector | `sparseVector` field | Convert `indices` + `values` to object keys. |
| Payload | Document fields | Merge into document unless configured under a nested object. |
| Payload index | `indexConfigs` entry | Map searchable/sortable fields only. |
| Qdrant `Cosine` | LambdaDB `cosine` | Direct mapping. |
| Qdrant `Dot` | LambdaDB `dot_product` | Direct mapping. |
| Qdrant `Euclid` | LambdaDB `euclidean` | Direct mapping. |
| Qdrant `Manhattan` | No direct equivalent | Fail by default unless user overrides. |
| MultiVector | Unsupported by default | Require explicit transform strategy. |

Field-name policy:

- LambdaDB field names cannot contain dots. Default: replace `.` with `_`.
- Detect collisions after normalization, e.g. `metadata.url` and `metadata_url`.
- Allow explicit field rename config to resolve collisions.

ID policy:

- Use the Qdrant point ID as the LambdaDB document `id` by default.
- Preserve ID semantics while adapting the type: Qdrant numeric ID `123` becomes LambdaDB document ID `"123"`, and Qdrant UUID IDs remain the same string value.
- Optionally copy the same original source ID into a separate field such as `_source_id` for audit/debugging, collection merges, or custom target ID strategies.

Sparse vector policy:

```go
func SparseToLambdaDB(indices []uint32, values []float32) map[string]float32 {
    out := make(map[string]float32, len(indices))
    for i := range indices {
        out[strconv.FormatUint(uint64(indices[i]), 10)] = values[i]
    }
    return out
}
```

## Collection Creation Strategy

V1 should support three modes:

1. `--create-collection=true`: inspect source schema and create a LambdaDB collection.
2. `--create-collection=false`: assume the target collection already exists.
3. `inventory`: print a proposed LambdaDB `indexConfigs` JSON/YAML without migrating data.

For Qdrant:

- Read vector params from `GetCollectionInfo`.
- Read sparse vector names from the collection config.
- Read payload schema from Qdrant payload indexes.
- Create LambdaDB vector index configs for each dense vector field.
- Create LambdaDB sparse vector configs for each sparse vector field.
- Create LambdaDB scalar/text index configs for selected payload indexes.

Payload index mapping:

| Qdrant payload schema | LambdaDB index type |
| --- | --- |
| Keyword / UUID | `keyword` |
| Text | `text` |
| Integer | `long` |
| Float | `double` |
| Bool | `boolean` |
| Datetime | `datetime` |
| Geo | V1 unsupported or stored as object |

Text analyzer defaults:

- Default to `standard`.
- Let users specify `english`, `korean`, and `japanese` in mapping config.

## CLI Shape

Top-level commands:

```bash
lambdadb-migration inventory qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.collection articles \
  --output inventory.yaml

lambdadb-migration qdrant \
  --qdrant.url http://localhost:6334 \
  --qdrant.api-key "$QDRANT_API_KEY" \
  --qdrant.collection articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --migration.batch-size 500 \
  --migration.write-mode bulk
```

Shared flags:

- `--migration.batch-size`
- `--migration.max-batch-bytes`
- `--migration.write-mode=bulk|upsert`
- `--migration.restart`
- `--migration.checkpoint-path`
- `--migration.batch-delay`
- `--migration.create-collection`
- `--migration.dry-run`
- `--migration.validate`
- `--mapping.file`
- `--debug`
- `--trace`
- `--skip-tls-verification`

Target flags:

- `--lambdadb.base-url`
- `--lambdadb.project-name`
- `--lambdadb.api-key`
- `--lambdadb.collection`
- `--lambdadb.wait-for-indexing`
- `--lambdadb.validation-timeout`

## Mapping File

Example:

```yaml
target:
  collection: articles
  createCollection: true

vectors:
  "":
    targetField: dense
    dimensions: 1536
    similarity: cosine
  title_dense:
    targetField: title_dense
    dimensions: 1536
    similarity: cosine

sparseVectors:
  sparse:
    targetField: sparse

payload:
  mode: flatten
  rename:
    metadata.url: metadata_url
  indexConfigs:
    tenant_id:
      type: keyword
    title:
      type: text
      analyzers: [english]
    created_at:
      type: datetime

ids:
  source: qdrant
  targetField: id
  copyOriginalTo: _source_id
```

The tool should generate this file from `inventory qdrant`, then let users edit it before migration.

## Pipeline Flow

1. Parse CLI and mapping file.
2. Connect to source and target.
3. Run `Inventory`.
4. Validate mapping against source inventory and LambdaDB limits.
5. Ensure target collection if requested.
6. Load checkpoint unless `--migration.restart`.
7. Read records from source in batches.
8. Transform records into LambdaDB docs.
9. Split docs by count and JSON byte size.
10. Write to LambdaDB with bulk or regular upsert.
11. Save checkpoint after each successful write.
12. Optionally validate counts and samples.
13. Delete checkpoint on success only if user passes `--cleanup-checkpoint`.

Checkpoint should be written after LambdaDB accepts the write. Because bulk upsert is asynchronous, V1 validation should distinguish "accepted by API" from "indexed and visible".

## Resumability

Default checkpoint location:

```text
.lambdadb-migration/checkpoints/<source-kind>/<source-id>/<target-project>/<target-collection>.json
```

For Qdrant sequential scroll, store:

```json
{
  "source": "qdrant",
  "sourceCollection": "articles",
  "targetCollection": "articles",
  "cursor": {
    "pointId": "42"
  },
  "acceptedRecords": 10000,
  "updatedAt": "2026-05-10T12:00:00Z"
}
```

Parallel migration can come after sequential V1. Qdrant's sampled range strategy is useful, but LambdaDB bulk writes and async indexing make error recovery harder. Start sequential, add parallel workers after correctness is proven.

## Validation

V1 validation:

- Source count vs accepted document count.
- Target `numDocs` polling after bulk indexing delay.
- Fetch a sample of migrated IDs and compare selected fields.
- Validate vector field existence and dimensions for sampled docs.

V2 validation:

- Side-by-side query comparison for dense, sparse, hybrid, and filter-heavy queries.
- Top-k overlap and rank correlation reports.
- Latency comparison report.

## Error Handling

Errors should be actionable:

- Unsupported distance metric: name the field and suggest mapping override.
- Field-name collision: list both source fields and normalized target field.
- Oversized document: print source ID and byte size.
- Oversized batch: auto-split unless a single doc exceeds max document size.
- LambdaDB 429: retry with backoff and suggest `--migration.batch-delay`.
- Bulk visibility delay: explain accepted-vs-indexed state.

## Test Strategy

Unit tests:

- Qdrant ID conversion.
- Dense, named dense, and sparse vector transform.
- Field-name normalization and collision detection.
- Index config mapping.
- Batch byte-size splitting.
- Checkpoint load/save.

Integration tests:

- Local Qdrant fixture to LambdaDB mock server.
- LambdaDB writer tests using `httptest` matching the SDK behavior.
- Optional real LambdaDB smoke tests gated by env vars:
  - `LAMBDADB_BASE_URL`
  - `LAMBDADB_PROJECT_NAME`
  - `LAMBDADB_PROJECT_API_KEY`

End-to-end fixtures:

- Single dense vector collection.
- Named vectors.
- Dense + sparse hybrid collection.
- Payload indexes with keyword, text, long, double, boolean, datetime.
- Bad cases: dot field names, Manhattan distance, multi-vector, oversized doc.

## Open Source And License Plan

Recommended approach:

- Create a fresh LambdaDB-owned repository.
- License the new project under Apache-2.0.
- Copy only necessary Qdrant source-adapter logic after simplifying boundaries.
- Preserve Qdrant copyright notices in copied or adapted files.
- Add a `NOTICE` or `ATTRIBUTIONS.md` entry.
- Add prominent "modified from" notices in files that carry substantial Qdrant code.

Suggested README acknowledgement:

```md
## Acknowledgements

This project includes code and design ideas adapted from
[qdrant/migration](https://github.com/qdrant/migration), Copyright 2024
Qdrant Solutions GmbH, licensed under the Apache License, Version 2.0.

Substantial changes were made to support migration into LambdaDB.
```

Suggested file header for adapted files:

```go
// Copyright 2024 Qdrant Solutions GmbH
// Copyright 2026 LambdaDB contributors
//
// Licensed under the Apache License, Version 2.0.
// This file includes code adapted from github.com/qdrant/migration and
// modified for LambdaDB migration targets.
```

This is not legal advice; before public release, have counsel confirm the attribution and NOTICE handling.

## Implementation Plan

### Phase 0: Skeleton

- Create Go module and CLI root.
- Add LambdaDB target config and SDK client wrapper.
- Add checkpoint file store.
- Add JSON/YAML mapping parser.
- Add dry-run output.

### Phase 1: Qdrant To LambdaDB MVP

- Implement Qdrant source inventory.
- Implement Qdrant sequential scroll reader.
- Implement Qdrant record-to-document transform.
- Implement LambdaDB collection creation from inventory.
- Implement LambdaDB bulk writer with byte-size splitting.
- Implement resumable checkpointing.
- Add unit tests and one local Qdrant integration test.

### Phase 2: Usability

Implemented:

- `inventory qdrant` command.
- Validation report and dense-vector query overlap checks.
- Progress and summary output.
- Docker image and GoReleaser snapshot/release configuration.
- README examples for Qdrant dense, named vector, and hybrid-style migrations.

Remaining:

- Extend query overlap validation to sparse, hybrid, and filter-heavy queries if needed.
- Add real-service controlled failure/retry coverage if LambdaDB exposes a safe fixture for 429/5xx behavior.

### Phase 3: More Sources

Add source connectors until the tool reaches parity with Qdrant's migration project. Prioritize implementation based on sales conversations:

1. Pinecone
2. Chroma
3. Weaviate
4. Elasticsearch/OpenSearch
5. pgvector
6. Milvus/Zilliz
7. Redis
8. MongoDB
9. S3 Vectors
10. FAISS
11. Apache Solr

Each new source should implement the same `Source` interface and produce neutral `Record` values.

## Key Changes From Qdrant Migration Source

- Replace target `commons.QdrantConfig` with `LambdaDBConfig`.
- Replace `connectToQdrant` target path with `lambdadb.New(...)`.
- Replace Qdrant `CreateCollection` request construction with LambdaDB `CreateCollectionRequest` and `IndexConfigsUnion`.
- Replace Qdrant offset collection with `checkpoint.Store`.
- Replace `PointStruct` creation with LambdaDB document maps.
- Replace `upsertWithRetry` with LambdaDB SDK calls plus 429/network retry handling.
- Replace Qdrant target `Count` with LambdaDB collection metadata polling.
- Remove Qdrant shard-key replication from V1.
- Keep source-side Qdrant scroll and collection inventory logic, adapted behind `source.Source`.

## Main Design Risks

- Bulk upsert async visibility may confuse users during validation.
- Payload schema inference from Qdrant only sees indexed payload fields, not every stored payload key.
- LambdaDB field-name restrictions require deterministic and explainable renaming.
- Very large payload fields may exceed LambdaDB document limits.
- Qdrant multi-vector workloads need custom migration strategy.
- Query relevance may change after moving hybrid fusion strategies.

## Recommended First Milestone

Ship a CLI that can:

1. Inventory one Qdrant collection.
2. Generate a mapping file.
3. Create the LambdaDB collection.
4. Scroll Qdrant points sequentially.
5. Transform dense, named dense, and sparse vectors.
6. Bulk upsert into LambdaDB.
7. Resume from a local checkpoint.
8. Produce a validation summary.

That gives the sales and success team a credible migration path without waiting for every source connector.
