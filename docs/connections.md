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
One worker process owns one connection's working files. Regional control is a
shared HTTPS API per enabled target region; S3 mode supports regional replicas.
The backend regional runtime owns managed Fargate launch and confirmed-stop
handoff. Standalone operators can launch the worker with a registered connection ID.

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
text or UUID primary-key column; INCLUDE attributes are not key columns.
Snapshots read ONLY the selected table, excluding traditional inheritance children.
Selected values support booleans, signed integers, text/varchar, UUID, JSON/JSONB and finite real/double. Reject selected
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
  FOR TABLE ONLY public.products (product_id, title, body)
  WITH (publish = 'insert, update, delete, truncate');
```

Configure source `wal_level=logical`, replication slot/sender capacity and WAL
retention monitoring. Continuous capture emits a transactional logical heartbeat
when no commit has been processed for five seconds. It requires permission to
execute the text overload of `pg_logical_emit_message` (normally granted to PUBLIC;
restore an explicit grant if revoked, using the installed PostgreSQL signature).
It writes no customer table rows. Only the heartbeat's decoded, locally persisted
commit advances feedback, never its SQL
return value or a keepalive's advertised WAL end. Paused, blocked and disconnected
workers do not generate heartbeats. Long transactions and outages can still retain
WAL: monitor `restart_lsn`, retained bytes and slot validity, and configure an
appropriate `max_slot_wal_keep_size` with a recovery plan for slot invalidation.
Use default replica identity and retain the primary key in the publication. Row filters, multi-table publications and schema changes are
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

Journal format 4 binds each target intent to a deterministic application plan.
Confirmed acceptance advances its cursor and managed-ID inventory atomically.
Snapshot/resync retries and replay of a decoded source transaction skip accepted
items. Continuous mode retains the next source row image before target application
and clears it atomically with the committed source checkpoint. Restart finishes
that retained plan before a newer resync. An unresolved sending intent still stops
as `target_outcome_uncertain`; the plan never authorizes its retransmission.

The new worker reads format 3 ordinary-API journals and writes format 4 on the
next update; older worker binaries refuse format 4. This does not authorize an
automatic downgrade or reinterpret older ordered-ingestion formats. Local storage
remains the default. Optional S3 worker recovery is described in
[connection-s3-state.md](connection-s3-state.md). The backend-owned
`docs/CONNECTION_S3_STATE_DESIGN.md` specifies the complete managed deployment;
bounded regional-replica and EFS-free Fargate qualification is recorded in
`docs/CONNECTION_S3_QUALIFICATION.md` there. This does not imply production rollout.

`connection.db` atomically persists immutable target intents, their transmission
state, a local journal sequence, accepted managed document IDs, initial/resync
cursors, approved references and observations. `source.db` retains source identity,
selected complete rows, checkpoint, slot ownership and current/prepared cuts.
Both are region-local, fsynced bbolt files protected by exclusive locks. Creation
syncs parent directory entries before the first remote observation; failure stops
initialization without claiming sequence one. This worker storage is independent of the engine's WAL storage; no EFS is required by
the worker. Local mode requires retained volume durability. S3 mode publishes
recovery authority remotely before these local transactions can authorize progress.

The worker calls ordinary supported collection creation and document upsert/delete
HTTP APIs. It sends no `ingestion`, connection association or idempotency-key
fields and requires no engine extension. A document HTTP 202 with a valid message
confirms acceptance. There is no per-write Fetch or search gate. Automatic retries
are limited to definite non-acceptance: local pre-send cancellation/authorization
failures and HTTP 429. Transport failures and ambiguous responses never retry.
Source checkpoint and replication feedback advance only after all target
operations in the source transaction have confirmed acceptance.

Before sending, the journal persists `prepared`, then `sending`. A restart can
send an intent marked `prepared`, which means it has not been accepted: it was
never sent, was rejected locally before transport, or received HTTP 429. An
intent marked `sending` without locally saved acceptance stops the connection as
`action_required` / `target_outcome_uncertain`. This includes transport failures,
HTTP 408/5xx, invalid success bodies, termination during transmission and failure
to save a successful response. A clear 3xx/4xx rejection other than 408 is recorded
as `target_rejected`, except HTTP 429; redirects are never followed. A 429 returns
the exact intent to `prepared` and persists its retry deadline. Retries honor
`Retry-After` (seconds or HTTP date), with a five-second minimum and exponential
worker backoff up to one minute; a longer server deadline is not shortened.
Transient throttling is reported as `target_unavailable`. A crash before recording
the definite rejection still leaves `sending` blocked conservatively. No failure
detail contains credentials or document content. Uncertain or permanent rejections
cannot be made retryable by restarting,
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
can be visible. The first snapshot is associated with its requested resync revision;
retained older cuts are never relabeled as a newer request on restart. Completion
certifies durable acceptance, not indexing visibility
or atomic query visibility. Customer writes can diverge or be overwritten;
conflict resolution and read/write key separation remain deferred.

Collection creation also persists an attempt before sending and confirmation after
success. An uncertain creation is never retried or adopted solely by metadata
lookup, even if a collection with that name is visible. Existing unrelated targets
are refused. A known created collection that disappears is not recreated. Direct
data-plane deletion/recreation is not fenced by this worker; central management
deletion guards remain, and operators must avoid replacing an active target.

Pause cancels and joins capture. An admitted target mutation drains under the
existing 30-second HTTP timeout, allowing its receipt to be saved during shutdown.
No new mutation starts after cancellation or observed authority expiry. Forced
termination, timeout or response loss can still leave a sending intent; uncertainty
takes precedence over paused/completed/disconnected status. Cached regional
authority permits ongoing replication during control outages until expiry; the
central backend is never in the batch path. Normal startup needs regional metadata.
Startup and polling enforce the same
command history: desired/resync revisions cannot regress, a revision cannot carry
a different command (including expiry), and disconnection is terminal. Authority
renewal therefore needs a newer desired revision.

Unchanged lifecycle and desired-state writes do not commit. Unchanged observations
are sent every five seconds; changes report on the next worker loop. Observation
sequences remain durable. Source row counts/LSNs use a transaction-serialized
in-memory summary rebuilt from retained state on restart, not a full row decode
per report. Preflight retains its per-commit validation but reads table/publication
metadata in one catalog statement instead of five round trips.

Disconnect without uncertain work stops capture and removes only an owned inactive
pgoutput slot. It makes no engine disconnect/seal call and preserves the collection.
Disconnection resolves no target credentials, and a continuous connection with no
retained slot or bootstrap ownership resolves no source credentials either, so an
unusable secret cannot block it.
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

## Actual worker in Fargate, October 28, 2026

A subsequent bounded Seoul run used this unchanged actual worker image against
PostgreSQL 17.11 and the actual regional HTTPS executable on a private EC2 test
fixture, plus a fresh collection in the approved LambdaDB project. Worker source
and journal files remained on a scoped regional EFS access point; real Secrets
Manager references supplied the source and project-key credentials. The fixture
submitted preapproved regional metadata and did not exercise the central HTTP/JWT
registration path, which has separate live management evidence.

Five full document/field comparisons passed: initial snapshot, ongoing changes,
offline changes after replacement, further changes and disconnected data. Cases
included exact bigint PKs, column projection, JSON replacement/empty objects, null,
delete/reinsert and PK changes. The first worker in ap-northeast-2a was confirmed
STOPPED, then a read-only CLI task verified accepted sequence 6 with no pending
intent. Source changes committed while the worker was absent. A non-initializing
replacement in ap-northeast-2b resumed the same collection/checkpoint. Final
inspection retained sequence 14, four managed IDs and resync revision 0, matching
the independent operation count without rewriting the baseline. Observation
sequences and source positions advanced. Disconnect removed the owned slot.

The backend-owned maintained harness and guide live under
`infra/connections/worker-validation`. Early harness routing/readiness failures
were fixed and preserved before any worker ran; no existing connection state was
reinitialized. This normal cross-AZ stop/restart test does not promote EFS to
production, validate replication during an NFS partition, certify a customer
provider or establish backup/restore, throughput or recovery-time guarantees.
The uncertain-write policy and earlier storage failure evidence still apply.

## Actual-worker fault qualification, October 28, 2026

The backend's opt-in `worker-validation/faults.py` passed a separate full Seoul
run against real PostgreSQL and LambdaDB. This repository adds only a test-only
supervisor in `internal/connections/fault_supervisor_test.go`, a separate
`Dockerfile.connection-fault-probe` QA image and its CI build/smoke. The image runs
the ordinary worker binary as a child and can SIGKILL it only on a private verified
TLS response bound to its exact ECS task ARN. It adds no fault hook to the product
worker, ECS Exec, Linux capabilities or shared PID namespace. Inspector tasks run
the actual read-only CLI directly.

The same worker recovered a scoped PostgreSQL network rejection, preserving its
checkpoint/feedback while blocked and catching up from six to eleven accepted
operations. A quiescent actual SIGKILL retained accepted sequence 11 with no
pending intent. After its task was confirmed STOPPED, source changes committed
while it was absent; a replacement in another AZ resumed the same collection.
Further changes reached accepted sequence 15 without rewriting the baseline.

A private test relay then forwarded operation 16 to real LambdaDB, received its
actual 202 and withheld every response byte. The supervisor SIGKILLed the waiting
worker; the client closed 0.478431 seconds after receipt. The journal retained
accepted sequence 15 and sending sequence 16, with the relay's exact request
digest and the prior source checkpoint. A third actual worker remained
`action_required/target_outcome_uncertain` across pause, resume, resync and
disconnect. Later source changes stayed unapplied; only 16 mutations and one
collection creation reached the target. Slot feedback stayed behind the uncertain
change, and the owned slot remained present. Final read-only inspection retained
the exact intent/checkpoint and resync revision 0. All six tasks stopped; the
three actual workers exited 137, 137 and 0. Test collection deletion was verified
as 404. Final infrastructure cleanup passed all 33 owned resource checks, with
zero remaining AWS stack resources. Fixture teardown removes the isolated
source/slot; it is not a completed
product disconnection or an uncertainty-recovery API.

Earlier reconnect-wait and stale-observation harness failures were preserved with
read-only state archives. Their fixes change only the backend validator. No
existing connection was reset or initialized again; retries used fresh resources.
Local full PostgreSQL race tests, `go vet ./...`, `go build ./...`, the amd64 QA
image build and executed container supervisor tests passed. The backend reports
22 fault/worker, 20 infrastructure and 8 coordinator tests plus lint passing.

This validates the bounded source outage, quiescent crash and lost-success-response
cases. It does not promote EFS, validate NFS partitions or interrupted commits,
provide automatic uncertain-write recovery, qualify backup/restore or load, or
provision production operation. Target readback remains a validator assertion,
not a production per-batch requirement. Engine and production worker Go behavior
are unchanged; ordinary supported LambdaDB APIs are used throughout.
