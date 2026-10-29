# Optional S3 recovery for connection execution

Local filesystem state remains the default. Existing standalone migration commands
do not require S3 or AWS credentials for state storage. The `connection-worker`
supports explicitly selected S3 recovery while retaining local bbolt working state.
Other features, including managed secret resolution, retain their own credentials.

S3 mode is required by the EFS-free managed task templates. Isolated snapshot and
continuous AWS API qualification passed on October 29; this is not a production
rollout. The detailed report and remaining load/retention/operations gates are in
`lambdadb-backend/docs/CONNECTION_S3_QUALIFICATION.md`. This guide describes the
worker. The cross-repository protocol is in `lambdadb-backend/docs/CONNECTION_S3_STATE_DESIGN.md`.

## Store initialization and worker startup

Use a regional general-purpose bucket with encryption and public access blocked.
The operator initializes its deployment prefix once:

```sh
connection-state -initialize-s3-store -region ap-northeast-2 \
  -s3-bucket example-connection-state -s3-prefix connections/test/
```

Record the returned `id`, pin it in deployment configuration, and do not leave this
action in a restart command. Existing markers are never overwritten. An unknown
initialization response requires inspecting that exact prefix, not choosing a
different identity. Workers have no permission to create/replace the store marker.

Run a newly registered connection once with `-initialize`:

```sh
connection-worker -control-url https://connections.example.com \
  -connection-id example-connection -state-dir /private/connection-cache \
  -state-store s3 -region ap-northeast-2 \
  -s3-bucket example-connection-state -s3-prefix connections/test/ \
  -s3-store-id '<pinned-store-id>' -state-epoch 1 -initialize
```

All subsequent starts omit `-initialize`. The approved binding must match the
store's region. Standard AWS SDK credentials are used only when S3 is selected;
managed tasks use scoped roles. The existing regional worker token and secret
references remain required. Never place raw source credentials or API keys in flags.

Managed task templates set `CONNECTION_REQUIRE_S3_STATE=1`; the worker refuses
local mode before opening state. The controller durably assigns `state_epoch`
and injects its value into the RunTask command. Standalone execution keeps its
local default and does not require that managed environment variable.

Exactly one process owns a connection. Before replacement, confirm the previous
actual process/task has stopped. The managed controller assigns the execution epoch;
standalone operators retain this responsibility when moving to another host and
advance `-state-epoch`. A timeout or expired lease is not stopped proof. Startup
claims the current head conditionally; stale owners cannot advance it. This does
not cancel HTTP requests already sent to LambdaDB, so uncertain target intents
always remain blocked, even with a higher epoch.

The cache root is locked locally before remote ownership changes. Each run uses a
new private `cache-*` subdirectory. On restart, the current S3 checkpoint and later
records reconstruct both `connection.db` and `source.db` before use. Existing cache
files and old archives are not recovery authority. Cache directories remain for
inspection on retained standalone disks; clean them only after confirming no owner
uses them. Managed task disks are ephemeral. A fresh cache requires no new source
baseline or collection. A lost, incomplete PostgreSQL exported snapshot still uses
the existing bootstrap failure policy.

## Durable ordering and failures

Both files share one serialized remote revision sequence. A local write exports a
logical state change, uploads an immutable record and conditionally publishes its
reference through `head.json` before committing locally. Successful local return
therefore requires remote durability. The head is never refreshed automatically
after a conflict or an unknown response; both database handles become unavailable.
After a remote commit and local failure, a fresh process rebuilds from S3.

Target preparation, sending, acceptance/cursor advancement, source transaction
staging, checkpoint/feedback and lifecycle ordering retain the same gates as local
execution. Recovery replays internal state only. A retained sending intent is
`target_outcome_uncertain`; neither current target contents nor a new process can
authorize resending it. S3 failures never fall back to local-only continuation.

Records use checksum-verified byte splices against the prior image. This preserves
opaque request bytes, payload digests and integers without JSON re-encoding. A new
staged source image can reuse the old source image as its base. Changes far apart
can include the intervening bytes; this first implementation does not promise
per-row upload cost or high throughput. Measure actual requests/bytes and latency.

On exit, an S3 worker emits one `connection_state_storage` JSON summary containing
SDK get/put call counts, failures, successful read bytes, attempted write bytes and
noncumulative latency buckets. No keys, paths, payloads, identities or secrets are
included. SDK retries are disabled; these counters describe SDK invocations and
are not an AWS billing receipt. Bucketed p50/p95/p99 are upper bounds, not precise
server-side performance measurements. Local mode emits no S3 metrics.

`Dockerfile.connection-s3-probe` builds a separate opt-in test image. The default
CI run needs no network and skips its live permission test. The isolated AWS runner
executes it under an actual connection worker role, using only fresh, unreferenced
canaries. It verifies own-prefix creation, foreign-prefix read/write denial,
regional-control denial, immutable overwrite denial, unconditional-write denial
and deletion denial. It never writes the active recovery head or runs capture.

A coherent checkpoint is published after 128 state records before further writes.
Recovery accepts at most 128 records and 128 MiB of replay payload bytes. Existing
32 MiB value and 10,000-row limits remain; combined recovery images are capped at
128 MiB. Published retained bytes are capped at 1 GiB per connection. Reaching a
limit stops execution. No garbage collector or lifecycle expiration is enabled.
Orphan uploads from failed publication and local caches require separate monitoring;
the published-byte counter does not claim to measure the entire bucket bill.
These are correctness-qualification limits, not unbounded production retention.

## Read-only inspection and permissions

```sh
connection-state -inspect-s3 -region ap-northeast-2 \
  -s3-bucket example-connection-state -s3-prefix connections/test/ \
  -s3-store-id '<pinned-store-id>' -connection-id example-connection \
  -fingerprint '<approved-binding-fingerprint>'
```

This reads the current head and verifies the complete checkpoint/log chain without
claiming ownership or writing S3. Temporary local materialization is deleted after
redacted inspection. Output has `remote_state_only: true` and
`resume_authorized: false`; it excludes row values, target request content and
credential references. Existing local inspection/archive commands remain available.
An archive restore stays inspection-only and cannot be activated as an S3 head.

Normal worker IAM permits `GetObject` on the pinned store marker and read/write on
that connection's worker prefix. It must deny writes to regional control, other
connections and the store marker, and deny deletion of objects and versions.
Bucket policy must enforce create-only immutable writes and conditional head writes.
Conditional writes are not a substitute for those access/deletion protections.
No cross-region fallback or automatic adoption of EFS state is included.
