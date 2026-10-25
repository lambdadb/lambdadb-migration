# Managed connection worker

This is the current execution path for generic managed `connections`. It replaces
private projects, generation replacement, search grants and write-time readback
in the historical `cdc-worker` experiment. The backend repository owns the
JWT management API, `connections` table, regional control and deployment templates.
This repository owns source capture and the separate `connection-worker` image.
No Console or production provisioning is included.

## Runtime

Build with `go build ./cmd/connection-worker`, or
`docker build -f Dockerfile.connection-worker -t connection-worker:local .`.
One worker process owns one connection's retained files. Regional control is one
shared HTTPS service per enabled target region. A scheduler is not implemented;
operators launch the first local spike's worker with the registered connection ID.

```sh
connection-worker \
  -control-url https://connections.example.com \
  -connection-id '<connection-id>' \
  -state-dir /state/<connection-id>
```

`CONNECTION_WORKER_CREDENTIAL` is a scoped token derived by the backend-owned
regional service's `-worker-token-for` provisioning command. Never give the
worker the regional operator token. The approved target region is the default;
`-region` or `AWS_REGION`, when present, must match it. Source and target secrets
are regional AWS Secrets Manager references resolved using the AWS SDK and task
role. Grant `GetSecretValue` only on the necessary source and target secret ARNs,
plus `kms:Decrypt` if their customer-managed KMS key requires it. The worker does
not create secrets, projects or cloud infrastructure. A target admin secret is
used internally; credential scoping is follow-up work.

For a **newly registered connection with no execution history only**, add
`-initialize` on its first invocation. Stop it and use the ordinary invocation
above for every subsequent run. The initialization flag must not be left on a
restarting service. Regional sequence-one initialization is claimed atomically,
source/journal files are created exclusively, and existing collections are never
adopted without a prior durable creation attempt. Volume loss is not repaired by
initialization. An absent, empty or mismatched file stops execution. Retain and
recover both files together; do not copy either active bbolt database.

The local-only options `-allow-environment-secrets` and `-allow-insecure-source`
allow `env:NAME` references and PostgreSQL `ssl_mode=disable`. They are disabled
by default. Source environment values use the same JSON credential shape as the
backend secret (`host`, `port`, `database`, `username`, `password`, `ssl_mode`);
the target value is the API key string. Do not put these values in command lines,
configuration JSON, status payloads or source control. `-ca-file` adds a trusted
CA for regional/target HTTPS without disabling verification. PostgreSQL uses
pgx's normal verified TLS/root-certificate configuration.

## PostgreSQL prerequisites

Use PostgreSQL 15+, one ordinary non-RLS/nonpartitioned table, and one integer,
text or UUID primary-key column. Selected values support booleans, signed
integers, text/varchar, UUID, JSON/JSONB and finite real/double. Reject selected
generated/unsupported types and non-key `id` fields. A customer chooses the source
settings, dataset, optional columns, mode, target name and valid nonempty index
configuration through the backend API. The collection defaults to the table name;
no new collection is chosen on restart. A source primary-key value becomes the
string document `id`; the original selected primary-key field is retained too.

For `snapshot`, provide `columns` if projection is needed and omit `publication`.
A complete repeatable-read transaction is retained before any document writes.
Only SELECT privileges are needed; no replication connection/publication/slot is
created. This imports that transaction's consistent cut, not writes after it.
A completed snapshot remains completed across restarts and authority expiry.

For `continuous`, an administrator first enables logical replication and creates
a dedicated one-table publication. Example setup, adjusted for actual identifiers:

```sql
-- Run with appropriate source administration privileges.
CREATE ROLE lambdadb_sync LOGIN REPLICATION PASSWORD '<source-secret>';
GRANT CONNECT ON DATABASE app TO lambdadb_sync;
GRANT USAGE ON SCHEMA public TO lambdadb_sync;
GRANT SELECT (product_id, title, body) ON public.products TO lambdadb_sync;
CREATE PUBLICATION lambdadb_products
  FOR TABLE public.products (product_id, title, body)
  WITH (publish = 'insert, update, delete, truncate');
```

Configure source `wal_level=logical`, replication slot/sender capacity and WAL
retention monitoring. Use default replica identity and retain the primary key in
the publication. Row filters, multi-table publications and schema changes are
rejected. The worker never creates or deletes a customer publication. The
publication's exact projection is selected for the initial exported snapshot;
optional API columns must match that set. Publication filtering limits emitted
data, not the database user's authorization. The worker reads logical pgoutput
operations, not physical page records or original SQL.

Customers must allow inbound PostgreSQL connectivity from the actual regional
worker's fixed egress addresses and enable suitable `pg_hba.conf`/TLS access.
The infrastructure operator supplies those addresses. This code does not assign
fixed egress IPs, update customer firewalls or provision customer-network agents.
A paused connection still retains WAL; monitor disk pressure and retention.

## Recovery and application

`connection.db` atomically persists pending immutable target intents, a monotonically
increasing producer sequence, the inventory of accepted managed document IDs,
small initial/resync cursors, approved references and observations. `source.db`
retains source identity, selected complete rows, source checkpoint, slot ownership,
and current/prepared cuts. Both are region-local, fsynced bbolt files protected by
exclusive locks. Filesystem/volume durability is required. This is not multi-host
HA; offset-only remote backup would lose necessary reconstruction state.

Use the target's supported document upsert/delete HTTP APIs with optional
`ingestion: {connectionId, sequence}`. Require the exact durable acceptance echo;
a generic 202 is insufficient. The supported Go SDK version does not yet expose
this additive field, so the adapter uses those same public HTTP endpoints. The
source checkpoint and feedback advance only after all target intents in that
transaction are accepted. There is no per-write Fetch or query gate. The caller
can use ordinary collection APIs/SDK immediately; partial initial/resync results
can be visible. Completion describes durable acceptance, not indexing visibility
or atomic cross-document query visibility. Customer writes may diverge from or
be overwritten by source changes; conflict resolution and read/write key separation
are deferred.

Normal restart validates the existing source/slot/projection, recovers an uncertain
intent, and streams from the retained checkpoint without rewriting the baseline.
Target ordering rejects a different producer/gap/conflicting latest sequence and
makes older accepted requests harmless, including delayed deletes after reinsertion.
It requires the companion engine contract; a version number alone does not prove
that all gateway/controller/writer nodes are upgraded. Do not enable a deployment
until every serving mutation path supports the new contract. Collection ownership
is checked at startup; document readback is only a test oracle.

Explicit resync retains the same collection and producer sequence. Save a new cut,
delete tracked managed IDs absent from it, put its rows with persisted cursors,
and promote the source checkpoint only after complete acceptance. The per-intent
ID inventory also removes rows accepted before a crashed source checkpoint. Rows
added only by customers are not automatically deleted. Slot continuity loss needs
explicit resync; incompatible source history/mapping requires a new connection.

Pause cancels and joins capture before reporting paused. Cached regional authority
permits ongoing replication during control outages until expiry; source feedback
and target sends check local authority. A restart currently needs the regional
service to read desired/observed metadata. Central backend is never in the batch
path. Authority renewal alone does not restart capture.

Disconnect stops capture, drops only a proven owned inactive pgoutput slot, seals
the producer through the target API, then atomically retires its association and
project deletion guard. A lost receipt retries those steps. The backend retires
the source secret and releases its central deletion guard after a matching receipt.
Target data remains readable, and terminal restart no longer needs either secret.
A source that owns a slot but is unreachable blocks normal cleanup; forced cleanup
needs a separate operator policy. Unknown target creation outcomes are recovered
by metadata lookup only. If the attempted collection is still absent, stop for
operator recovery rather than risk a late duplicate create. No automatic orphan
secret/target reconciliation is claimed.

## Bounds and validation

This first correctness delivery keeps at most 10,000 rows, 32 MiB per retained
source dataset/journal state and 8 MiB of logical transaction input. One complete
row is sent per ordered intent. Initial/resync datasets are not rewritten into the
common journal on every target operation. This is a bounded spike, not production
throughput or unbounded outage/HA certification. MongoDB, Console, credential
rotation, remote state/backup and production orchestration remain later work.

Run local maintained source/worker tests using `bash scripts/cdc-test-postgres.sh`
(the script creates and removes its own PostgreSQL container). The optional actual
engine component test is:

```sh
CONNECTION_TEST_BACKEND_DIR=/path/to/backend-worktree \
CONNECTION_TEST_ENGINE_DIR=/path/to/engine-worktree \
bash scripts/connections-test-engine.sh
```

It builds both executables, starts real backend regional control, local PostgreSQL,
and the engine's test-only HTTPS component target. The target uses public DTOs,
real catalog/routing, mutation handler, writer overlays and EFS WAL; its HTTP
adapter and project/catalog storage are fixtures. It does not replace deployed
Gateway/indexer/DynamoDB/S3/native-image validation. It proves real SIGKILL recovery,
unchanged collection/baseline, late upsert, delete/reinsert, lost responses,
partial-apply resync, direct collection deletion protection, preserving-data
disconnect and late-write rejection after disconnect. WAL/catalog unit tests
separately cover cursor recovery and real DynamoDB Local transactions. Evidence
contains only local fixture credentials. The test never deploys or calls a cloud
target. A configured Seoul target cannot prove an undeployed engine extension.
