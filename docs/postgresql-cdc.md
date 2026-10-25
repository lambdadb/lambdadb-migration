# PostgreSQL CDC correctness spike

> The October 26 accepted product direction is defined in the backend repository's
> `docs/CONNECTIONS.md`: self-service generic connections, snapshot/continuous modes,
> publication column parity, a fixed normal customer collection and ordinary SDK
> search. Historical new-generation and search-gate behavior below is still present
> in code until its replacement is validated; it is not the new product default.

This separate executable, connector implementation, correctness tests and worker
image are owned by `lambdadb-migration`. The management API remains central in
`lambdadb-backend`, which also owns regional infrastructure and deployment
configuration. The worker has no management HTTP server, backend database
connection, or per-batch central API dependency.
The backend now supplies central create/get/control endpoints and a separate
regional HTTPS control service. The worker caches approved commands and reports
metadata regionally. Production provisioning remains deferred. See the backend
`docs/CDC_WORKER_CONTRACT.md` for authorization, outbox and wire semantics.
Do not expose this executable as a customer-operated connector.

`worker_region` defaults to `target_region`; this spike rejects a different region.
This is placement metadata, not infrastructure provisioning or proof of physical
placement. Provision the executable, state disk and secret access in that region
before a managed pilot. The live test ran on a developer machine against a Seoul
target, so it does **not** certify a deployed Seoul worker or its network route.

## Smallest implementation and guarantees

1. Preflight one ordinary PostgreSQL 15+ table, its single-column primary key and
   a dedicated publication with insert/update/delete/truncate. A column list
   selects the exact snapshot/stream projection and must retain the primary key.
   Reject RLS, partitions, generated columns, filters and unsupported types.
2. Create a persistent `pgoutput` slot using `EXPORT_SNAPSHOT`. Import the slot's
   exported snapshot before reading any table data in a read-only repeatable-read
   transaction. Hold the exporting replication connection throughout the scan.
3. Save the complete baseline and the matching consistent point atomically in a
   regional bbolt database. The source slot retains concurrent WAL; no customer
   outbox, row rereads, engine WAL access or central replay storage is used.
   No source acknowledgement precedes the durable baseline. A durable ownership
   marker permits dropping only this binding's incomplete-snapshot slot and
   rebuilding from a fresh slot/snapshot pair after reader failure.
4. Create a unique target collection on **every process run**, then rebuild its
   baseline from regional row state. Never reuse a previous collection name.
   This deliberately avoids needing in-place target fencing for the spike.
   Old requests can still modify an abandoned collection, but cannot address a
   successor collection. There is no automatic serving-route publication.
5. Decode committed transactions in order. Reconstruct full row images from
   baseline plus changes, including unchanged TOAST and primary-key changes.
   Collapse a source transaction to its final per-key state. Upsert complete new
   images before deleting removed keys. Use ordinary SDK upsert, never the
   recursively merging update endpoint or asynchronous bulk ingestion.
6. Serialize individual target operations and verify each through branch
   `Fetch(consistentRead=true)`. Only then atomically persist new row state and
   transaction **end** LSN. Only that recoverable checkpoint is sent in replication
   feedback. A crash after local save but before feedback resumes from local LSN;
   replayed older transactions are skipped before reconstruction.
7. On uncertain/failed target requests, stop before newer writes or checkpointing.
   Reopen the regional state and recover into a fresh collection. Lost partial
   transactions are reread from source WAL, with their prior complete row images
   still on disk. Configuration/schema/source-identity drift and slot loss fail
   closed. Authority expiry cancels the worker locally.
8. Emit a unique transactional logical message after baseline reconstruction and
   consume its committed transaction through `pgoutput` with `messages=true`.
   At that transaction end LSN, pause application, verify the complete committed
   target PK/field set against regional row state, and atomically select the
   generation only if its execution epoch and expected previous selection match.

Progress separates `durable_lsn` from `applied_lsn` during rebuilding. `streaming`
means replication has started, **not** caught up or Live. Neither applied LSN nor
pending-inclusive document reads are a committed/searchable watermark. `verifying`
means a committed source barrier has been consumed. `active` means the local
generation selection passed complete committed-document verification; it is not
a serving route, current liveness signal or BM25/vector search watermark. Source
feedback continues during verification, but newer transactions are not applied
until verification and selection finish. Ordinary replication then continues.

The barrier is one source WAL message per worker run, not a customer outbox/table
write, central API call or sampled `pg_current_wal_lsn()` used as snapshot origin.
Its expected state is reconstructed from the original consistent snapshot and
ordered committed changes, not a fresh source SELECT. Tests separately compare
the final source PK/field set at a quiescent point. The source role must be allowed
to call `pg_logical_emit_message`; failure stops selection. The behavior follows
PostgreSQL's [logical message function](https://www.postgresql.org/docs/17/functions-admin.html#FUNCTIONS-REPLICATION)
and [pgoutput message option](https://www.postgresql.org/docs/17/protocol-logical-replication.html).

`execution` metadata in the same regional bbolt file records monotonically
increasing epochs, the candidate collection, authority expiry, immutable verified
LSN/digest and the selected generation. A newer epoch invalidates old verification
and activation requests, even if the older request arrives later. Selection uses
compare-and-swap against the expected previous epoch and rejects source-progress
regression. The prior selection is retained as historical metadata while a new
candidate rebuilds. Neither its presence nor an old `active` report authorizes
serving it after a restart, outage or authority expiry. A future regional resolver
must enforce the selected epoch, authority/liveness and search-availability policy;
central telemetry copies cannot become selection authority.

Verification currently compares the supported committed listing responses; it
does not pin an immutable target ref or certify snapshot consistency across pages.
Serving promotion must separately establish the required query/ref visibility
contract. The local selection must not be used as that missing proof.

The complete bounded mirror is intentionally simple: 10,000 rows, 32 MiB serialized
state, an approximately 16 MiB baseline data budget and 8 MiB decoded transaction
budget. Whole-state copy/serialization and per-document visibility polling are not
a throughput design. Pending changes during snapshot/application remain in source
WAL, not a separately provisioned regional queue. Retention exhaustion stops
continuity; the spike does not promise unlimited-outage recovery.

## Source and mapping

Publication columns are selected in physical table order for both the exported
snapshot and pgoutput decoding. Unpublished values never enter the row mirror or
target; their unsupported/generated types do not block the selected projection.
Row filters remain rejected. Supported selected PostgreSQL types are `bool`,
`int2/int4/int8`, `text/varchar`, `uuid`, `json/jsonb`, and finite `float4/float8`.
Primary keys must be integer, nonempty text, or UUID. Target `id` is the source PK
rendered as a string. A non-key source `id` column is rejected. Other column names
and JSON values are retained. SQL NULL and JSON null both map to target JSON null;
their source distinction is not preserved. Inference, pgvector, arrays, numeric,
timestamps and schema evolution require separate mapping work.

Only index the fields appropriate to the test data. LambdaDB rejects null indexed
values; this spike does not silently omit or coerce them. Unindexed nullable fields
are retained, and JSON replacement includes empty objects and key removal. Native
embedding schema configurations are outside the supported spike configuration.
The SDK receives decoded JSON values with `json.Number`, since SDK v0.7.1 treats a
`json.RawMessage` nested in a document as an array of byte values. A wire-level
regression test protects this boundary. SDK reads use floating-point JSON numbers;
exact comparisons fail closed when the SDK cannot round-trip a large integer.

Prepare only isolated test resources, for example:

```sql
CREATE TABLE public.cdc_items (
    id bigint PRIMARY KEY,
    title text,
    body text,
    metadata jsonb
);
CREATE PUBLICATION cdc_spike_publication FOR TABLE public.cdc_items
    WITH (publish = 'insert, update, delete, truncate');
```

Enable logical replication and grant the SQL/replication privileges needed by the
test role. The worker creates its own slot; it does not adopt an existing slot.
The publication is selection, not a credential isolation boundary.

## Build and local deployment

From the `lambdadb-migration` repository root:

```sh
go build -o /tmp/lambdadb-cdc-worker ./cmd/cdc-worker
/tmp/lambdadb-cdc-worker -config /absolute/path/binding.json -state /absolute/path/checkpoint.db
```

Copy `lambdadb-backend/infra/cdc/binding.example.json` outside either repository
and supply the real isolated target URL/project, source names, secret environment
references, region and a
short-lived `authority_expires_at`. The example intentionally requires editing.
Keep its binding revision unchanged during recovery. The data-identity fingerprint
excludes authority expiry; changing mappings still requires explicit resync. This
standalone file path is trusted local input. Managed execution uses the regional
authenticated protocol described below.

```sh
docker build -f Dockerfile.cdc-worker -t lambdadb-cdc-worker:spike .
docker run --rm lambdadb-cdc-worker:spike -help
```

The backend-owned `infra/cdc/compose.yaml` consumes this image through
`CDC_WORKER_IMAGE`; it does not build worker source. Its local configuration also
requires `CDC_BINDING_FILE`, `CDC_STATE_DIR`, `CDC_SOURCE_DSN` and
`CDC_TARGET_API_KEY`. Managed deployment must use an immutable image digest; local
validation may use the explicit build tag above. This image is not published by
the migration CLI's GoReleaser/Homebrew release process. The state directory must
be writable by UID 65532, retained across restarts, and reside on a single host's
durable regional disk. A local source
on the Docker host needs `host.docker.internal`, not container `localhost`.
The database is mode 0600; encryption/backup/access policy belongs to the regional
deployment. Never copy a running state database or place it on a network filesystem.
The bbolt exclusive file lock coordinates processes sharing this disk, not a
multi-host fleet. Logs contain binding/generation/offset summaries, not rows or
credentials. No automatic restart storm or collection garbage collection is enabled.

## Managed control mode

```sh
# Secrets are resolved from protected environment references in the worker region.
/tmp/lambdadb-cdc-worker -control-url https://REGIONAL_CONTROL_ORIGIN \
  -binding-id BINDING_ID -control-token-env CDC_CONTROL_TOKEN \
  -state /absolute/path/checkpoint.db
```

The regional control token is binding-scoped; the worker never receives the central
API JWT, database connection or regional operator credential. Commands carry an
immutable binding `revision` and separate monotonic `desired_revision`. Authority
renewal before expiry extends the same epoch without restarting capture. Completed
pause is reported only after the run exits. Resume, expiry recovery and process
restart use the retained row image/WAL and a new collection/epoch. An expired or
paused epoch cannot be revived. Mapping changes remain explicit resync work.

`managed_control` is an additive bbolt bucket containing the last accepted bundle
and persistent observation sequence. The binding fingerprint excludes authority
expiry and uses canonical JSON. A provable legacy hash is upgraded atomically for
checkpoint, bootstrap and execution metadata without changing row images or LSNs.
Managed saves and target operations recheck authority. Delayed commands cannot
regress desired revision; duplicate identical commands are idempotent. Regional
status includes the acknowledged revision; stdout `observed_revision` remains the
immutable binding revision for standalone compatibility.

The supervisor polls only the regional service (one-second interval, two-second
HTTP timeout), independently of capture/apply. A cached unexpired command permits
restart during both central and regional delivery outages. No cache means startup
requires regional availability. The initial central grant is 15 minutes, renewed
explicitly for at most one hour per command; there is no autonomous renewal.
No per-batch HTTP call is required. `action_required` does not automatically create
new target generations; a new desired revision or process restart permits retry.

This cadence does not certify a revocation SLO or cancel mutations already accepted
by the data plane. The backend contract describes queued/delivered/applied states,
stale observations, allowlisted regions and remaining multi-host/serving gates.

## Optional bounded regional search

The managed worker can also host a regional TLS search gate in its own process.
This uses its authoritative state and capture lifetime, without sharing bbolt or
trusting delayed central telemetry. Enable it explicitly with `-search-listen`,
`-search-tls-cert`, `-search-tls-key` and a separate `CDC_SEARCH_TOKEN` (at least 32
bytes, different from `CDC_CONTROL_TOKEN`). Backend owns the optional
`infra/cdc/serving-compose.yaml` deployment override. The normal default is off.

`POST /v1/bindings/{id}/search` accepts only `field`, `term` and optional `limit`.
The current predicate supports a default text index, ASCII words/spaces or null,
one lowercase alphanumeric term, at most 100 source rows and four in-flight
queries. The supported SDK uses committed queryString search with `skipSyntax`.
Every matching PK and field must equal the independently derived regional
expectation before limiting. Any mismatch returns 503. The response includes a
predicate-specific `validated_lsn`, not a collection-wide searchable watermark.

Startup/rebuild/run exit, paused/expired authority, stale capture health and a
superseding epoch block admission. Apply invalidates outstanding queries without
waiting for their remote requests. The final response check validates epoch,
authority, checkpoint and mutation serial. SIGKILL removes the listener; restart
must recover and select a new generation. There is no old-candidate fallback.

This route does not revoke project keys or raw collection/ref access. The current
data plane authenticates those keys at project scope. Protect target credentials
and require authoritative application authorization before any production pilot.
Source freshness, immediate source ACL revocation, broad analyzer/vector support,
query ranking and multi-host fencing remain separate gates. See backend
`docs/CDC_VALIDATION.md` for exact commands and local/live evidence.

## Application-server signed grants

The optional search listener now also supports `-search-verification-keys` pointing
to a retained local public Ed25519 JWKS. With this flag, the earlier static token
is never accepted; missing/invalid/empty key files fail startup. The worker has no
signing seed and performs no HTTP key lookup or central authorization call.

The backend owner/admin API issues binding-wide read grants for trusted application
servers, with a 60-second maximum capped by execution authority. Claims bind the
binding ID, immutable mapping fingerprint, target region and control revision, with
fixed purpose, issuer, audience, algorithm and scope. JWT `sub` is the delegating
management member or registered `cdc-search-client:CLIENT_UUID`, never an
end-user/tenant identity. Timing and current control are
checked before target reads and again before releasing data. Grant issuance is not
proof of serving readiness; the existing per-query row/state checks still gate it.

Valid grants work offline until expiry. A later control revision invalidates old
grants, including after pause/resume and renewal. Capture continues under its own
cached authority if app grants expire. New grants need central availability. The
trusted issuer must retain its management JWT. The backend also registers
binding-scoped application credentials; the application exchanges one at
`POST /auth/cdc/search-grants` and gives only the resulting grant to the worker.
The application credential is never a worker control or data-plane project key.
Existing project keys retain direct target access and source/tenant ACLs remain
independently gated. Public key changes are loaded at startup; overlapping-key
rotation/restart requires retained state and a separately qualified rollout.

See backend `docs/CDC_APPLICATION_AUTH.md` and
`infra/cdc/search-grants-compose.yaml` for exact scope, commands and evidence.

## Correctness validation

```sh
bash scripts/cdc-test-postgres.sh
go vet ./...
go build ./...
```

The shell script starts a fresh PostgreSQL 17 container on a random localhost port,
runs `go test -v -race -count=1 -timeout 5m ./...`, and removes the container. Tests
use unique schemas/publications/slots and temporary state directories. Without
`CDC_TEST_POSTGRES_DSN`, database tests explicitly skip; ordinary unit tests alone
are not the PostgreSQL proof. Live LambdaDB is opt-in and skips in regular CI.

On Unix, the suite also builds the actual `cmd/cdc-worker` executable with race
instrumentation and the diagnostic `cdctest` build tag. Deterministic boundaries
notify the parent and stop the child; the parent sends real `SIGKILL`, checks the
termination signal, reopens the disk and restarts a new child. No Go error or
defer simulates these deaths. Eight windows cover snapshot reading, target apply
before save, save before feedback, baseline rebuild, catch-up, verified-but-not-
selected state, selection transaction before commit and selection after commit.
The SDK speaks TLS to an isolated test target which survives child death. The
opt-in live suite additionally kills real workers after actual LambdaDB writes
and after checkpoint save, then verifies a replacement generation in Seoul.

Only diagnostic binaries support `CDC_TEST_STOP_AT`, `CDC_TEST_BOUNDARY_FILE` and
the local test CA. Normal CLI/Docker builds contain no stopping or CA-override
behavior. State and fault controls never appear in management configuration. The
test target is not proof of engine durability; the live cases are recorded
separately in the validation document.

To reuse an isolated localhost PostgreSQL for the live test:

```sh
docker run --detach --name lambdadb-cdc-spike-pg \
  --publish 127.0.0.1:55439:5432 \
  --env POSTGRES_PASSWORD=cdc-local-only --env POSTGRES_DB=cdc_spike \
  postgres:17-alpine -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8
CDC_TEST_POSTGRES_DSN='postgres://postgres:cdc-local-only@127.0.0.1:55439/cdc_spike?sslmode=disable' \
  python3 scripts/cdc-test-live.py --env-file .env.local
docker rm -f lambdadb-cdc-spike-pg
```

The env file supplies `LAMBDADB_BASE_URL`, `LAMBDADB_PROJECT_NAME` and
`LAMBDADB_PROJECT_API_KEY`; it is parsed as literal values, never evaluated as shell
code or printed. Live tests create distinct `cdc_test_*` collections and remove
their own generations on exit. They never clear an existing collection/project.
Do not point them at a production project. See
the backend `docs/CDC_VALIDATION.md` guide for cross-repository checks and remaining
gates. Keep dated local runs and failure-investigation records outside commits.

## Remaining managed-service gates

- Central create/get/control, JWT/project authorization, explicit eligibility,
  authenticated regional delivery and stale observed metadata are implemented
  locally. Full CRUD, automated secret distribution, worker provisioning,
  removal/resync, audit history and certified revocation bounds remain.
- Backend-owned regional production IaC, encrypted state/backup, multi-host
  ownership/fencing, source WAL budgets/alerts and retention policies remain.
- The opt-in bounded search route selects the current validated execution. General
  serving publication, collection-wide searchable watermarks, direct-access
  enforcement and old-generation cleanup remain unimplemented. Old collections remain independently writable. No engine change is
  proposed; in-place recovery would require separately validated fencing/ordering.
- No source failover/restore certification, resharding, provider/network matrix,
  automated source TLS policy, credential rotation, long-outage/load tests or
  freshness SLO. Test grants are short-lived, not indefinite central-outage leases.
- Real SIGKILL and local regional-service interruption are tested. Host/power loss,
  torn writes, disk loss, source-server crash and an interruption of a deployed
  central API remain untested.
- Console UI and production provisioning remain deferred.

## Explicit lifecycle cleanup and resync

The backend connection list, DELETE and POST resync endpoints are documented in
its `docs/CDC_LIFECYCLE.md`. Both lifecycle mutations require the expected desired
revision and `discard_replica:true`. They use control protocol 2; binding identity
and search-grant protocol remain unchanged. Upgrade the regional binary and
worker before issuing these commands. Older workers can retain their cached v1
permission only until expiry; command delivery is never cleanup completion.

Production target names are journaled in the regional bbolt file before SDK
collection creation. The bounded inventory holds at most 128 names, including
unconfirmed creates. Cleanup never discovers ownership by listing a prefix.
A name collision quarantines the journal; legacy volumes without a complete
inventory require manual reconciliation and refuse automated destruction.
Ordinary restarts keep old generations until explicit cleanup.

After capture exits and serving is cleared, an authorized regional cleanup task
verifies source ownership, refuses active/unowned slots and incompatible-schema
resync, drops its slot and deletes only recorded collections through SDK Delete.
It requires typed GET 404 before removing a journal entry. Uncertain responses
retain local recovery state. Already absent slots/collections permit idempotent
restart. Do not independently reuse connector slot or collection names.

The final local transaction clears row/bootstrap/selection state and records a
terminal removal or completed resync-cleanup revision, retaining execution epoch,
control and observation sequence. Resync then starts the existing consistent
snapshot and complete verification path into a fresh generation. Source rows,
publications, unrelated collections and the target project remain untouched.
A resync is unavailable until the replacement passes validation; it does not
repair incompatible mappings or erase remote backups. Removal remains terminal.

Cleanup rechecks cached authority before mutations/reset, has a 30-second task
deadline and stops on pause/expiry/supersession. Failure reports `action_required`
until a new command revision or process restart. No per-batch central call is
introduced. Collection absence does not prove physical reclamation or fence a
previously accepted remote create/mutation; those engine contracts remain open.

Maintained tests exercise real SIGKILL at planned-create, slot-drop, target-delete,
pre-reset, in-reset and post-commit boundaries; authority/inventory/epoch fences;
unowned/foreign source slots and incompatible schema; response-loss/404 retry;
and final preservation of unrelated resources. Run `TestLambdaDBLiveLifecycle`
with the existing protected-file launcher to validate the real supported SDK and
worker against an isolated target. The live test also supports
`CDC_TEST_REGIONAL_BINARY` for the actual backend regional process over TLS.

## Offline inventory inspection and reviewed repair

The worker includes two maintenance modes for an existing stopped regional volume:

```sh
cdc-worker -inspect-inventory -state /state/checkpoint.db
cdc-worker -reconcile-inventory /protected/reviewed-inventory.json -state /state/checkpoint.db
```

The first opens state read-only and emits validated metadata, known generation
names and an exact state digest, without source values, secret references or
credentials. It refuses a running worker's lock and missing/invalid state. The
second atomically records an explicitly reviewed complete inventory and receipt;
it never starts capture or contacts a source/target. Both modes run separately
from capture/search options and require the original retained regional volume.

Repair requires cached unexpired central pause/removal authority, a fenced epoch,
exact binding/desired revision/state digest, all previously recorded names and an
explicit `ownership_confirmed:true` operator attestation. Its strict manifest is
capped at 16 KiB and 128 unique production-shaped names. Independent creation/audit
evidence must prove each name and completeness, including abandoned generations.
Matching prefixes and current target listings do not establish ownership. Known
name collisions, omitted names, changed state and corrupt/foreign identities fail
closed. Older ambiguous history still requires manual investigation.

Only the inventory changes. Rows/checkpoint, snapshot ownership, epoch, selection,
control and authorization remain intact. A durable digest/revision/timestamp
receipt makes response-loss retry idempotent. The normal managed worker performs
remote cleanup only under the existing central lifecycle command. An inventory
receipt alone cannot mark connection removal complete or revive a removed binding.

See the backend `docs/CDC_INVENTORY_RECOVERY.md` for the operator sequence, expired
authority handling and a deliberately unconfirmed manifest template. Keep real
manifests, state copies and evidence out of source control. Maintained tests include
actual CLI SIGKILL before/after the bbolt commit and subsequent central-authorized
owned-only cleanup with real local PostgreSQL and a TLS supported-SDK fixture.

## SQL-only snapshot reader

`postgrescdc.ReadSnapshot` reads a bounded, repeatable-read source cut with ordinary
SELECT privileges. It creates neither a publication nor a logical slot and returns
only after the complete read transaction commits. Explicit columns must include
the single primary key and obey the same type/generated-column and `id` mapping
rules as continuous capture. An omitted column list selects all table columns.
Snapshot mode can use non-default replica identity because it does not decode WAL.

The projected `SELECT ... LIMIT 0` retains the source relation's ACCESS SHARE lock
through the transaction, including when the credential can SELECT only chosen
columns. Explicit `LOCK TABLE` would require wider table privileges. Both snapshot
readers reuse projection validation and row decoding; their existing 10,000-row /
regional-state budgets remain in force. The source's later commits are excluded
from the returned baseline. A new invocation establishes a new cut; the future
connection runner must persist the complete baseline before target application
and must not re-invoke this reader on an ordinary completed-snapshot restart.

This source-reader stage is exercised locally, including a NOREPLICATION login
with column-only grants and no publication. It is not yet a `connection-worker`
CLI, target application, restart journal or a deployed snapshot import feature.

## Fixed-target intent journal and API transport

`internal/connections` is a new common regional execution package. Its bbolt
journal requires explicit one-time initialization; restart opens an existing file
without `O_CREATE`, rejects uninitialized/changed state, and retains exclusive
single-volume ownership. It persists a complete upsert/delete intent and sequence
before sending. Exact target acknowledgment clears that intent atomically; an
uncertain outcome remains pending and must be recovered before another operation.
JSON content retains integer precision and canonical escaping across persistence.

The target transport uses the documented document HTTP API paths and existing
`x-api-key` authentication. Shared gateways additionally receive the registered
native project ID and version. It sends optional `ingestion` metadata and requires
a matching 202 acceptance echo; generic success, wrong order and redirects never
advance the journal. Normal application does not call Fetch or a central backend.
The existing Go SDK remains available for ordinary collection operations; its
current generated mutation DTOs do not yet expose this engine extension.

The pure metadata protocol is mirrored from the backend's internal connections
protocol because independently built repositories cannot import Go internal
packages. Keep its wire version, field names and validation aligned when changing
it. Source adapters keep source positions inside the common source-type/offset
shape. The new journal/transport is not yet a runnable managed worker: target
ownership, producer sealing, source capture orchestration and lifecycle receipts
still need integration. The older worker retains its historical generation flow.

Validation includes real subprocess SIGKILL before/after acknowledgment, missing
state, exclusive access, large integer/escaped content, response loss, wrong
acknowledgment and redirect behavior. HTTP transport fixtures prove the client
contract, not deployed engine support; durable ordering is independently tested
in the engine's EfsBranchWal/WriterMutationCommitter tests. No new ordered API was
deployed to the Seoul endpoint by these changes.

## Capture without target generation management

`postgrescdc.Capture` separates PostgreSQL execution from target generation/search
behavior. Explicit `Initialize` persists a complete exported snapshot and owns a
connection-derived slot; `Resume` requires the retained checkpoint and never starts
a new baseline. `Stream` reuses pgoutput row/TOAST/PK reconstruction, checks mapping
continuity, and invokes durable target acceptance before saving rows/LSN and sending
source feedback. It has no central backend calls, collection creation or readback.
Requested columns must match the publication before a slot is created.

`Disconnect` removes only a durably owned, inactive pgoutput slot after source
identity/configuration checks; it leaves the customer's publication/table intact.
The managed restart open path for both source state and the common intent journal
refuses missing or empty/truncated files. An initialized but absent source checkpoint
cannot be treated as a new snapshot by `Resume`. An offset alone cannot restore row
reconstruction state.

Tests prove retained-baseline restart, pending WAL replay, target rejection before
checkpoint advance, selected-column TOAST/PK changes and owned slot cleanup. This
is still a source adapter and common journal/transport, not a completed managed
worker deployment. Idle unrelated database WAL and outage slot-retention limits
need operational monitoring; this bounded capture only acknowledges processed,
durably accepted source positions, never a keepalive's advertised WAL end.

## Managed document ownership during partial application

Common journal format 2 records a bounded set of managed document IDs atomically
with each accepted intent. It supports one document per upsert/delete intent in
this first delivery. A pending operation is not certified as accepted until its
matching target receipt; recovery accepts the same intent and updates ownership
in the same local transaction. Rejected payloads cannot advance either sequence
or ownership. The inventory remains regional and contains no credentials.

This inventory is necessary for same-collection resync: a partially applied source
transaction can add a target row before the source row/LSN checkpoint advances.
A fresh source cut must be able to delete that row if it no longer exists, even
though it is absent from the older source checkpoint. Keep managed IDs across
restart, recover pending intent first, and reconcile deletions before insertions.
Customer-created IDs are not silently adopted into this inventory; arbitrary
customer writes remain outside the first delivery's conflict guarantees.

Older development journal formats are rejected explicitly. Do not initialize a
new journal to repair a missing one or attach it to an existing producer/target.
This is a local format change before worker rollout, not an in-place migration of
an operating deployment. The existing 10,000 managed-row / 32 MiB journal budgets
remain explicit limits of the correctness implementation.
