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

`connection.db` atomically persists immutable target intents, their transmission
state, a local journal sequence, accepted managed document IDs, initial/resync
cursors, approved references and observations. `source.db` retains source identity,
selected complete rows, checkpoint, slot ownership and current/prepared cuts.
Both are region-local, fsynced bbolt files protected by exclusive locks. This
worker storage is independent of the engine's WAL storage; no EFS is required by
the worker. Retained volume durability is required; remote state and HA are later.

The worker calls ordinary supported collection creation and document upsert/delete
HTTP APIs. It sends no `ingestion`, connection association or idempotency-key
fields and requires no engine extension. A document HTTP 202 with a valid message
confirms acceptance. There is no per-write Fetch, search gate or automatic write
retry. Source checkpoint and replication feedback advance only after all target
operations in the source transaction have confirmed acceptance.

Before sending, the journal persists `prepared`, then `sending`. A restart can
send an intent still marked `prepared`, since transmission never started. An
intent marked `sending` without locally saved acceptance stops the connection as
`action_required` / `target_outcome_uncertain`. This includes transport failures,
HTTP 408/5xx, invalid success bodies, termination during transmission and failure
to save a successful response. A clear 3xx/4xx rejection other than 408 is recorded
as `target_rejected`; redirects are never followed. No failure detail contains
credentials or document content. Retrying cannot be enabled by restarting,
changing desired state, resync or authority renewal. No manual journal-clear or
readback-based acceptance shortcut is supplied. Investigate/quiesce any late
request before planning operator recovery; a delayed ordinary write can still
reach the target while the connection is stopped.

Journal format 3 deliberately refuses previous ordered-ingestion journal formats.
Existing experiment files must be retained for investigation; do not delete them,
initialize over them or attach a new journal to an existing target. The previous
engine-specific experiment and generic EFS idempotency prototype are deferred,
not required by this execution path.

Normal restart with no uncertain request validates source continuity and retains
the same collection/baseline. Explicit resync reconciles accepted managed IDs and
persists cursors in the same collection, only when no uncertain/rejected intent
blocks execution. Customer-only IDs are not removed. Partial initial/resync data
can be visible. Completion certifies durable acceptance, not indexing visibility
or atomic query visibility. Customer writes can diverge or be overwritten;
conflict resolution and read/write key separation remain deferred.

Collection creation also persists an attempt before sending and confirmation after
success. An uncertain creation is never retried or adopted solely by metadata
lookup, even if a collection with that name is visible. Existing unrelated targets
are refused. A known created collection that disappears is not recreated. Direct
data-plane deletion/recreation is not fenced by this worker; central management
deletion guards remain, and operators must avoid replacing an active target.

Pause cancels and joins capture. If cancellation leaves a sending intent, uncertainty
takes precedence over paused/completed/disconnected status. Cached regional
authority permits ongoing replication during control outages until expiry; the
central backend is never in the batch path. Normal startup needs regional metadata.

Disconnect without uncertain work stops capture and removes only an owned inactive
pgoutput slot. It makes no engine disconnect/seal call and preserves the collection.
The backend retires the source secret and releases central deletion guards only
after a matching successful regional disconnection observation. An uncertain
connection cannot certify disconnection or release those guards. An unreachable
source with an owned slot requires a separately defined operator cleanup policy.

## Bounds and validation

This first correctness delivery keeps at most 10,000 rows, 32 MiB per retained
source dataset/journal state and 8 MiB of logical transaction input. One complete
row is sent per locally journaled intent. Initial/resync datasets are not rewritten into the
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

It builds both executables and starts actual regional control, PostgreSQL and the
engine's test-only HTTPS component target. The existing runner is supplied by the
separate deferred engine experiment; these tests reuse only ordinary collections
and writes, with no ingestion/association metadata. The engine checkout remains
unchanged. The target uses real DTOs, catalog/routing, mutation handling, writer
overlays and EFS WAL; its HTTPS adapter and project/catalog storage are fixtures.
Using that harness does not require its optional ordering feature in the worker,
and does not validate the planned EFS replacement or a deployed service.

Current process tests prove ordinary continuous insert/update/delete and safe
restart keep the same collection. SIGKILL/restart stops uncertain upserts/deletes,
and lost responses remain blocked. Later source changes are not sent, management commands
cannot bypass uncertainty, safe snapshot restart/resync keeps the collection,
and missing source volumes are refused. A held ordinary request can later reach
the target while the worker stays stopped. These tests do not claim delayed-write
fencing, direct data-plane deletion protection or automatic uncertain recovery.
Separate unit/local-PostgreSQL tests cover successful-response journal-save failure,
prepared-but-unsent continuation, uncertain collection creation, ordinary insert/
update/delete, source-checkpoint preservation and legacy journal rejection.
No cloud deployment, live Seoul test, production load or native-image validation
is included in these results.

## Local worker against the live Seoul API, October 28, 2026

A scoped live run used actual local `connection-regional` and `connection-worker`
executables, an isolated PostgreSQL 17 source and the existing Seoul v3 data-plane
endpoint. The worker was built from migration `1c15d0f`; regional control was built
from backend `1356e0d`. Only unique, previously absent test collections were used.
The worker called ordinary collection/upsert/delete APIs. A separate validation
runner compared every committed document and field through the public list API;
these reads are test assertions, not per-batch worker verification.

Nine checks passed: continuous initial snapshot/projection, insert/update/delete,
retained-state restart with offline PK changes and delete/reinsert, pause/resume,
continuous disconnect and terminal restart without source/target credentials,
snapshot-only import, completed snapshot restart without importing later changes,
explicit resync in the same collection, and snapshot disconnect/terminal restart.
The initial large bigint PK was preserved exactly as both the string document ID
and selected numeric source field. Excluded source fields were absent. Both final
collections were deleted with HTTP 200 and subsequent HTTP 404 was verified. An
initial runner attempt had a process-observation ordering error; its test
collection was also confirmed absent. Local processes and containers were removed.

This verifies ordinary live API compatibility and bounded data correctness, not
production deployment, throughput or a freshness SLA. Initial live source data was
quiescent; concurrent exported-snapshot handoff and uncertain-request faults remain
covered by separate local tests. Local regional desired metadata was published
directly for this run; central JWT registration, AWS Secrets Manager/IAM, managed
worker scheduling, fixed egress, shared-gateway native project-ID routing, remote
state/HA, and a live ambiguous-write recovery scenario were not exercised. No
engine change, private target, request-idempotency facility or EFS receipt feature
was required. Secret values, retained execution state and one-off validation
scripts/logs remain outside source control.


## Incident inspection

Stop the worker and its automatic restarts, then inspect the retained journal and
source files together without control or AWS credentials:

```sh
connection-worker -inspect-state -state-dir /retained/connection
```

The result is bounded local metadata with request content, document IDs and secret
references excluded. Both files are opened read-only; missing/corrupt/locked or
mismatched state is refused. `resume_authorized` is always false. The command does
not initialize, clear uncertainty, retry or contact a service. Follow the
[incident inspection and recovery procedure](connection-recovery.md); current
reads or elapsed time cannot authorize recovery from an uncertain remote write.

## Experimental Fargate/EFS qualification

`Dockerfile.connection-volume-probe` builds a separate Go test image that exercises
the actual journal and PostgreSQL source store with synthetic data. Its
`TestStateVolumeProbe` supports explicit `hold`, `contend` and `verify` phases
through `CONNECTION_VOLUME_PROBE_DIR` and `CONNECTION_VOLUME_PROBE_PHASE`.
The ordinary suite exercises those phases in separate local processes under
`TestStateVolumeProbeLocal`. No PostgreSQL, target or secret lookup is performed.

The backend's `infra/connections/efs-validation` owns the isolated Fargate/EFS
qualification stack and runner. Its two-AZ experiment checks lock exclusion and
retained state after process termination; successful local tests do not qualify
EFS or prove fencing during a network partition. Production state remains a
retained local volume until those gates are evaluated. This experiment does not
change normal worker behavior or the LambdaDB engine, and does not authorize a
deployment or automatic retry of an uncertain request.

The October 28 approved Seoul Fargate/EFS run passed the three worker phases and
three regional phases on actual EFS, with holders in AZ a and contenders/verifiers
in AZ b. The worker recovered its exact source row/cut/LSN, accepted inventory
and `sending` intent after holder termination, and refused retransmission. The
regional probe preserved uncertainty and rejected stale or clearing observations.
All tasks stopped and the isolated AWS resources/images were cleaned up and
verified. The run used synthetic state and EFS-only task permissions; it did not
exercise PostgreSQL capture or LambdaDB writes. This bounded result leaves
network-partition fencing, interrupted commits, growth/performance and real data
movement across Fargate replacement as remaining gates. Normal worker storage
and the source store's local-filesystem restriction remain unchanged.

`TestEFSPartitionProbe` is a further opt-in journal experiment compiled into the
same test image. The backend's `partition.py` coordinates task-local TCP 2049
interruption, a cross-AZ ownership attempt, reconnecting the still-alive original
handle, and post-stop inspection. Agent requests are constrained to the task-local
ECS address and NFS port; normal worker code is unchanged. API success must be
accompanied by blocked synchronous I/O. A stale handle's successful write while
a replacement owns the journal is a storage fencing failure, not a reason to
automatically retry or add connector-specific engine behavior.

The actual October 28 partition experiment blocked NFS I/O and admitted a
replacement file owner while the old task remained alive. After reconnect, the
old transaction returned an error; explicit reuse of that handle caused SIGBUS
inside bbolt. The failed journal was archived read-only. Fresh cross-AZ and local
verifiers retained accepted sequence 2, both managed IDs and no pending intent;
the archived journal also retained baseline writer data. No stale-write success
or durable regression was observed.
This is storage-probe evidence, not an actual replication-worker run or a reason
to change the engine. Normal storage remains unchanged. Live-owner takeover is
unqualified; actual worker replacement awaits an explicit handover/I/O-error
policy, including confirmation of the old task's STOPPED state for a singleton
approach. All owned cloud resources were cleaned up after evidence preservation.

## Storage errors and execution handover, October 28, 2026

The source store and connection journal now serialize transactions through a
shared fail-stop handle guard. A begin/commit failure marks that handle unusable;
subsequent reads and writes return a redacted storage error without entering
bbolt again. Callback rejections such as expired authority or an existing pending
intent retain their previous behavior. This adds no on-disk fields or limits.
The worker returns on storage failure even when target handling has translated
an acceptance-save error into target uncertainty. Failed phase writes are no
longer ignored. Regional HTTP failures still permit capture under cached authority.

This contains returned storage failures, not failures inside the first mmap access
or blocked NFS calls. SIGBUS remains process-fatal. A blocked or failed task must
be terminated and confirmed STOPPED before another task accesses the retained
files; a newly available file lock is not proof that the previous task stopped.
The backend's regional execution controller must enforce this ordering. Do not
use an expiring ownership lease, rolling overlap or a restarting service with
`-initialize`. Actual EFS worker deployment remains unqualified.

Focused coverage includes a commit whose bytes survive but which reports EIO,
waiting readers/writers refused after failure, ordinary callback rejections, and
an actual worker receiving a successful target response while its source/journal
handle becomes unavailable. These are deterministic local tests, not a repeat of
the earlier cloud network-partition experiment.

The backend controller's separate Seoul experiment subsequently confirmed actual
STOPPED before a replacement journal verifier was created, including a blocked
NFS write. Task termination in that fault case took about 178 seconds and recorded
a container runtime stop timeout. This is synthetic storage/controller evidence;
the actual replication worker was not run in Fargate in that experiment. It does
not promote EFS or promise a recovery time. The backend guide records the exact
boundary and conditional-claim race result.
