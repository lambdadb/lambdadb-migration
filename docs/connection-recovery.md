# Managed connection incident inspection

This procedure covers retained local state for the ordinary-API connection worker.
It does not authorize a pending-request reset, automatic retry or target adoption.
The management API stays central; recovery authority is the retained regional
source/journal pair. The inspection tool makes no network calls.

## Contain and preserve

1. Stop the connection's worker and disable its automatic restarts/scheduler first.
   A central pause request alone does not prove that an in-flight request ended.
   Preserve the current management/regional observations and desired revision.
2. Retain `connection.db` and `source.db` from the same execution together. Keep
   their original files, permissions and binding. Do not copy an active bbolt file,
   combine files from different backups, remove them or use `-initialize`.
3. Run inspection with read access to both stopped files:

   ```sh
   connection-worker -inspect-state -state-dir /retained/connection
   ```

   This needs no control URL, worker token, AWS credential or source credential.
   Both files are held read-only together. Missing, empty, incompatible, corrupt,
   mismatched or locked state is refused; inspection cannot initialize files.
4. Store the JSON result in a restricted incident directory. It contains connection
   ID, mode/region, desired and local phase, accepted local sequence, managed-ID
   count, pending operation/status/sequence/digest, and source cut/row-count/LSN
   metadata. It excludes row values, document IDs, endpoint/project details and
   secret references. Original state files still contain row data; do not upload
   those files or dump pending content into logs/tickets.

`source.lsn` is a PostgreSQL source position in standard hexadecimal LSN notation.
`accepted_sequence` and `pending.sequence` are local journal coordinates, not
PostgreSQL LSNs or LambdaDB WAL positions. `source.rows` counts retained source
images and does not certify target acceptance or search visibility. The digest
identifies immutable local request bytes; it is not a server acceptance receipt.

## Interpret the evidence

| State | Interpretation | Current allowed action |
|---|---|---|
| `prepared` | Non-acceptance is established: never sent, locally rejected before transport, or HTTP 429 persisted with its retry deadline. | Normal worker restart may send the same intent after its deadline and authority checks. Never manually clear or relabel it. |
| `sending` | The request may have reached the target without locally saved confirmation. | Keep execution blocked. Do not resend, resync, initialize or disconnect to bypass it. |
| `rejected` | A definite HTTP rejection was saved. | Investigate configuration/authorization and preserve evidence. No journal-clear/retry command is supplied. |
| Saved `target_outcome_uncertain` | Uncertainty was independently retained, even if acceptance bytes later survived. | Keep execution blocked; absence of `pending` does not clear this condition. |
| Creation attempted without confirmation | A collection may already exist or a delayed create may still arrive. | Do not adopt by name, retry create or substitute a new journal. |
| No pending request | No unresolved intent is present in this pair. | This alone does not prove source continuity, volume freshness or target identity. Normal restart still performs its existing checks. |
| Invalid/missing/old-format/mismatched files | Required recovery authority cannot be established. | Retain evidence and investigate the original volume/binding. An older backup or current target read alone is insufficient. |

The output always has `local_state_only: true` and `resume_authorized: false`.
`recovery_required: false` is not permission to resume; it only describes the
bounded local conditions inspected. The tool has no `clear`, `accept`, `retry`,
`reset`, `rebind` or remote-mutation option and never changes the worker's gate.

## Uncertain remote outcomes

Collect the approved connection ID, local request digest/sequence, operation,
last source LSN/cut, desired revision, process lifecycle and operator-controlled
target-side evidence. Escalate to the owner of the data-plane acceptance path.
Do not disclose source/target credentials or row content during that handoff.

Before any future recovery mechanism can issue writes, authoritative evidence
must establish both the request's actual acceptance outcome and that no delayed
attempt can race later writes. The present API does not expose a per-request
durable receipt or a cancellation/fencing proof. A matching current document,
HTTP timeout, process exit, elapsed delay or successful retry is insufficient.
Do not manufacture such proof by editing a database flag or operator checkbox.

The current implementation therefore keeps uncertain outcomes blocked. Even if an
operator obtains independent acceptance/quiescence evidence, this tool does not
apply it or override regional uncertainty. A recovery mutation/command requires
a separately reviewed contract and implementation. Revisit generic idempotent
acceptance with the replacement WAL design, not an EFS-specific connector feature.

While blocked, retain the PostgreSQL slot and monitor its WAL/disk retention.
Source WAL exhaustion or continuity loss is a separate incident; do not silently
advance feedback, drop a customer publication/slot, release deletion guards or
retire credentials to make the connection appear complete. Forced cleanup and
resuming from older backups require a separate operator decision and protocol.

## Bounded local commit interruption qualification

A Linux-only QA image exercises actual process death at the first and second
`fdatasync` calls in a journal transaction. A test subprocess locks its OS thread
so strace's per-thread injection counter identifies the selected data/meta sync.
The test requires recorded SIGSTOP, the exact syscall count and actual SIGKILL,
then reopens the existing journal and runs bbolt's page-integrity check. Acceptance
and 1 MiB checkpoint-growth transactions are separate cases. Expected results are
whole old/new states: pending sending evidence and inventory cannot be partially
cleared, and checkpoint bytes cannot be mixed. The growth case verifies the file
actually grew from 32 KiB to 2 MiB rather than testing only small cached updates.

```sh
docker build -f Dockerfile.connection-crash-probe -t connection-crash-probe:local .
docker run --rm --cap-add=SYS_PTRACE --env CONNECTION_TEST_COMMIT_CRASH=1 \
  connection-crash-probe:local \
  '-test.run=^Test(CommitCrashAtDataAndMetadataSync|Journal|Inspection)' \
  '-test.v' '-test.timeout=2m'
```

Only this local/CI container receives SYS_PTRACE. It is never a deployed worker
image, and the ordinary journal/bbolt dependency has no new injection hook. The
four syscall cases passed on native Linux arm64. Normal journal/inspection tests
run alongside them to check subprocess lifecycle interactions. This is process
crash evidence, not host power-loss, EFS durability or old-backup freshness proof.
The operating system remains alive with its page cache. In particular, surviving
metadata bytes alone do not establish a completed fsync under physical failure.

Reference: [strace syscall injection](https://man7.org/linux/man-pages/man1/strace.1.html).

## Paired backup and inspection-only restore

After the regional controller confirms the original task STOPPED, use the separate
`connection-state` operator executable. The ordinary worker image also includes `connection-state`; override its entrypoint
for an operator task, retaining the confirmed-STOPPED precondition. It needs no
source/target secret, control
URL, AWS credential or network access. It never authorizes a resumed worker.

```sh
go build -o /private/bin/connection-state ./cmd/connection-state
/private/bin/connection-state -state-dir /retained/stopped-connection \
  -backup /private/backups/new-state.tar.gz
/private/bin/connection-state -restore /private/backups/new-state.tar.gz \
  -state-dir /private/incidents/new-inspection-directory
connection-worker -inspect-state -state-dir /private/incidents/new-inspection-directory
```

Backup validates the pair and keeps both original read-only locks through copying.
A same-directory temporary file is synced before exclusive publication; an
existing destination is never overwritten. The private 0600 gzip/tar contains
only connection.db, source.db and a format-one JSON manifest with lengths, SHA-256
checksums and redacted inspection metadata. The databases themselves contain row
data and credential references: keep the archive private and do not upload it to
review tools or tickets. Archive members are bounded to 512 MiB per state file;
this is an archive safety limit, not an increased worker state budget.

Restore requires a new 0700 directory, refuses links/traversal/duplicate or unknown
members, verifies exact lengths/hashes, gzip CRC and the pair's actual inspection
against the manifest, and syncs the output files. Failed copies stay quarantined
for diagnosis. The durable `.restore-inspection-only` marker is created before
extracting either database. The worker refuses both ordinary restart and
initialization from any directory carrying this marker, before contacting control
or resolving secrets. No marker-clear/activate/automatic restore API is supplied.

An intact clean backup can still be older than writes accepted by LambdaDB, or
omit a sending intent saved after that backup. Hashes and a matching local source
LSN do not establish freshness against the remote acceptance history. The marker
therefore applies to every restore, including a clean old pair. Inspection,
archiving and restored inspection never clear uncertainty, advance source
feedback, drop a slot or release management deletion guards. Retain normal
same-volume restarts for existing state; old-backup activation needs a separately
reviewed recovery contract. File locks do not fence an old NFS owner, so the
external confirmed-STOPPED precondition remains required before backup.
