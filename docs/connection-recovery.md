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
| `prepared` | The retained intent has not begun transmission under this journal format. | Preserve the files. The normal worker can attempt its first send only after its ordinary authority/source validation succeeds. |
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
