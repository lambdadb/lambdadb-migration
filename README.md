# LambdaDB Migration

CLI tooling for migrating vector databases and search systems into LambdaDB.

Supported sources include Qdrant, Pinecone Serverless, and Elasticsearch. LambdaDB is the only target.

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
lambdadb-migration elasticsearch --help
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

## Homebrew

Install stable `v0.1.6` from the
[public LambdaDB tap](https://github.com/lambdadb/homebrew-tap):

```sh
brew install lambdadb/tap/lambdadb-migration
lambdadb-migration --version
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

The formula uses the existing release binaries and requires no separate Go or
Node installation. Before switching from `install.sh`, inspect
`type -a lambdadb-migration` and preserve any existing executable; never
force-overwrite it or run the standalone installer over a Homebrew-managed path.
See [Homebrew installation and maintenance](docs/homebrew.md) for migration
between installers, verification, platform coverage and the manual tap update
procedure. Public installation was verified on macOS arm64 and Linux amd64;
macOS amd64 and Linux arm64 archives have checksum/header verification only.

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

Pinecone operations are namespace-scoped. If `--pinecone.namespace` is omitted, the connector reads Pinecone's default namespace only; it does not iterate over every namespace in the index. For multi-namespace indexes, run one migration per namespace or use separate LambdaDB collections. Pinecone can store the same vector ID in different namespaces, so merging multiple namespaces into one LambdaDB collection requires an explicit ID strategy such as prefixing IDs with the namespace.

## Elasticsearch To LambdaDB

Set your Elasticsearch API key and LambdaDB connection values:

```bash
export ELASTIC_API_KEY="your-elasticsearch-api-key"
export LAMBDADB_BASE_URL="your-region-specific-lambdadb-base-url"
export LAMBDADB_PROJECT_NAME="your-lambdadb-project-name"
export LAMBDADB_PROJECT_API_KEY="your-project-api-key"
```

Generate an inventory and editable mapping from an Elasticsearch index:

```bash
lambdadb-migration inventory elasticsearch \
  --elasticsearch.url https://your-elasticsearch-endpoint \
  --elasticsearch.index articles \
  --output elasticsearch-inventory.yaml
```

Review `elasticsearch-inventory.yaml`, then run a dry-run:

```bash
lambdadb-migration elasticsearch \
  --elasticsearch.url https://your-elasticsearch-endpoint \
  --elasticsearch.index articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file elasticsearch-inventory.yaml \
  --migration.dry-run
```

Run the migration with validation:

```bash
lambdadb-migration elasticsearch \
  --elasticsearch.url https://your-elasticsearch-endpoint \
  --elasticsearch.index articles \
  --lambdadb.base-url "$LAMBDADB_BASE_URL" \
  --lambdadb.project-name "$LAMBDADB_PROJECT_NAME" \
  --lambdadb.api-key "$LAMBDADB_PROJECT_API_KEY" \
  --lambdadb.collection articles \
  --mapping-file elasticsearch-inventory.yaml \
  --migration.write-mode bulk \
  --migration.validate \
  --migration.validation-report validation-report.json
```

The Elasticsearch connector inventories index mappings, maps supported scalar fields and `dense_vector` fields, reads documents with point-in-time `search_after` pagination, and fetches dense vectors explicitly with the search `fields` parameter. Nested `_source` objects are flattened into dot-path payload fields before LambdaDB field-name normalization.

Elasticsearch point-in-time IDs can expire. If a resumed migration fails because the saved PIT is no longer valid, rerun the migration with `--migration.restart`.

## Text Analyzer Mappings

Text indexes support the 49 fixed presets from LambdaDB server merge
`55d888299fee44466326a9db8016af9811ade13b` (PR #437), retained in Go SDK v0.7.0:

```text
standard english korean japanese arabic chinese cjk french german hindi
indonesian italian portuguese russian spanish turkish armenian basque bengali
brazilian bulgarian catalan czech danish dutch estonian finnish galician greek
hungarian irish latvian lithuanian norwegian persian romanian serbian sorani
swedish thai simple whitespace stop keyword pattern fingerprint nepali tamil telugu
```

For example, edit an inventory mapping to include:

```yaml
payload:
  indexConfigs:
    title:
      type: text
      analyzers: [french, german]
    raw_title:
      type: text
      analyzers: [keyword]
    category:
      type: keyword
```

Names are case-sensitive; order and duplicates are preserved. Omitted/null
`analyzers` uses the server default `[standard]`; an explicit `[]` remains empty.
The `keyword` analyzer is a text preset, distinct from the `keyword` field type.
Unknown names, non-string values, custom pipelines and text index options beyond
`type`/`analyzers` are rejected by mapping validation and target schema creation.

Elasticsearch inventory reads both `/_mapping` and `/_settings`; the source
credentials must permit both reads. It preserves unmodified supported field
analyzers and resolves an index default or named alias containing only a built-in
`type` to that fixed preset. Without a configured default it leaves analyzers
omitted; an explicit field `analyzer: default` resolves to `standard`.
Custom/configured definitions and unsupported names produce a warning and an
`unsupported:<source-name>` analyzer marker. Generated mappings fail validation
until explicitly edited; a reviewed manual mapping can select a fixed preset
without silently assuming the custom source analysis is equivalent.
Field `search_analyzer`, `search_quote_analyzer` and index `default_search`
settings are reported as warnings and are not translated. Multi-fields retain
an explicit warning. Migration runs, including dry-runs, print inventory warnings
to stderr. Review these differences before migrating.

The field/index default precedence follows the
[Elasticsearch analyzer contract](https://www.elastic.co/guide/en/elasticsearch/reference/8.19/specify-analyzer.html).
Preset names do not promise identical tokenization across engines or versions.
Nepali/Tamil/Telugu are LambdaDB Lucene extensions, not shared Elasticsearch or
OpenSearch presets. Plugin analyzers require manual review; Korean/Japanese/Chinese
LambdaDB names are not treated as shared built-in source names. This repository
has an Elasticsearch connector, not a separate OpenSearch adapter; OpenSearch
compatibility is not established by this change.

Qdrant text tokenizer/options are reported as untranslated; generated text indexes
use LambdaDB standard unless the mapping is edited. Pinecone metadata indexes
remain unintrospected with the existing warning. Neither source supplies analyzer
presets that can be assumed equivalent to LambdaDB.

Dense/sparse query-overlap verification remains ordinary retrieval and never adds
reranking. SDK publication and server source commits do not establish deployment:
confirm the intended target supports these presets before an actual migration.

## Common Options

`inventory qdrant`, `inventory pinecone`, and `inventory elasticsearch` write YAML for `.yaml`/`.yml` outputs and JSON otherwise. `--mapping-file` accepts either JSON or YAML, as a direct mapping object or as the wrapped output produced by an inventory command.

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

The target uses LambdaDB Go SDK `v0.7.0`. Collection creation returns HTTP 201 with collection metadata; the current API has no `collectionStatus` readiness field to poll. Transient write errors use the configured retry policy. Bulk uploads forward the signed headers returned by LambdaDB (including `If-None-Match: *`) and send `Content-Type: application/json`. A retried upload obtains a fresh URL; checkpoints advance only after every write for a source batch succeeds.

Checkpoints record source exhaustion separately from validation success. Re-running a completed checkpoint skips source reads and writes, while requested validation and cleanup still run. Validation-enabled migrations retain up to `--migration.validation-sample-size` sample documents in the local checkpoint so failed validation can be retried without re-uploading; checkpoint files use owner-only permissions. If a completed checkpoint has no saved samples, requesting sample validation requires `--migration.restart` (or sample size `0` for count-only validation).

Older checkpoint files remain readable and resume from their saved cursor. They have no completion marker, so completion is not inferred from document counts. For a migration already completed with an older version, use `--migration.restart` to start a fresh run and checkpoint. Use restart for new source data after a completed migration as well.

`--migration.validation-report` writes a JSON report with pass/fail status, source and accepted counts, LambdaDB `numDocs`, sampled document IDs, compared sample count, query overlap results, and validation errors. Setting it also enables validation.

`--migration.query-overlap` adds dense and sparse vector query overlap checks for validation samples when those vector mappings are present. By default it reports overlap without failing; set `--migration.query-overlap-min-ratio` above `0` to require a minimum average overlap. Query-overlap validation currently supports Qdrant and Pinecone sources; Elasticsearch migrations can use count/sample validation, but query-overlap is not implemented for Elasticsearch yet.

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

## Native Embedding Mappings

Go SDK [v0.7.0](https://github.com/lambdadb/go-lambdadb/releases/tag/v0.7.0)
(tag commit `fd3000bcb4353b3d6ed23fcca4f111955b36b33e`) supports the native
embedding contract pinned to backend `9072a1bc8925954369a887f558f1eaf387b7ea0e`.
To explicitly generate an additional vector from migrated text, add this to a
reviewed mapping and select `--migration.write-mode upsert` (JSON input is also
supported):

```yaml
payload:
  mode: flatten
  rename:
    source_body: body
  indexConfigs:
    body:
      type: text
    generated_vector:
      type: vector
      embedding:
        provider: openai
        model: text-embedding-3-small
        sourceField: body
```

`embedding.sourceField` names the **destination** field after payload renaming
and normalization. The CLI preserves it literally. Omitted `managedEmbedding`
enables native embeddings; no flag, dimensions or similarity default is inserted.
For explicit dimensions or similarity, put them inside `embedding`:

```json
{
  "type": "vector",
  "embedding": {
    "provider": "openai",
    "model": "text-embedding-3-small",
    "sourceField": "body",
    "dimensions": 256,
    "similarity": "cosine"
  }
}
```

Legacy `managedEmbedding: true` is accepted and preserved, including normalized
server metadata. Explicit `managedEmbedding: false` with `embedding`, top-level
native dimensions/similarity and unknown options are rejected. Structural errors
are reported during mapping validation and schema construction; model and
source-field compatibility remain server validations with SDK error types intact.

Generated inventories continue to migrate stored vectors through `vectors`, with
the existing dimensions and similarity defaults. Pinecone integrated embedding
configuration is not copied. A native payload index cannot replace a mapped source
vector field: the existing collision check rejects that mapping. Upsert and bulk
writes preserve caller vectors and source text, without client embedding calls,
vector removal or schema-driven rewriting. Existing collections are left intact;
this tool has no schema update path.

**Native generation is an explicit migration choice.** On a compatible backend,
regular upsert of text to a collection with a native field can invoke the provider,
including retries. The pinned backend rejects bulk upsert for collections with
native embedding fields; select `--migration.write-mode upsert` explicitly.
The CLI retains its existing bulk default and propagates the server rejection. Native fields reject directly supplied vectors; keep
stored vectors in ordinary vector fields to preserve migration fidelity. This
also applies to pre-existing target collections, whose schema the CLI does not
reconcile. The CLI does not suppress generation or promise provider-call
idempotency. Count/sample validation checks migrated fields; it does not validate
the quality of additional generated vectors.

Query-overlap validation still uses caller-vector KNN and sparse retrieval. It
omits top-level `candidateSize` and rerank, preserves server result order, and
cannot validate a native field via `queryVector`. There are no Bayesian queries,
rerank requests or score-processing paths to migrate here; Bayesian is not a
migration default. For separate SDK queries, follow the
[Bayesian/native embedding guide](https://github.com/lambdadb/go-lambdadb/blob/v0.7.0/docs/bayesian-native-embeddings.md).

The SDK's dev validation does not establish this CLI's target deployment. Before
live migration, verify the actual running backend revision and compatible schema.
For live tests use the authorized temporary-project/key workflow, keep credentials
and signed URLs out of logs/files, revoke keys, delete temporary resources and
verify absence while preserving persistent CI resources.

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
go run . inventory elasticsearch --help
go run . qdrant --help
go run . pinecone --help
go run . elasticsearch --help
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
