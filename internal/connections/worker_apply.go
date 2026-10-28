package connections

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
)

func stringRevision(revision uint64) string { return strconv.FormatUint(revision, 10) }
func (w *Worker) ensureTarget(ctx context.Context, target *APITarget) error {
	d, err := w.data()
	if err != nil {
		return err
	}
	if d.CreationAttempt && !d.TargetCreated {
		return ErrTargetOutcomeUncertain
	}
	if time.Now().Before(d.CreationRetryAt) {
		return &retryTargetError{d.CreationRetryAt}
	}
	meta, err := target.Describe(ctx)
	if err != nil {
		return errors.New("target_unavailable")
	}
	if meta != nil {
		if !d.TargetCreated {
			return errors.New("target_conflict")
		}
		return nil
	}
	// A missing formerly created target or uncertain creation is not safe to
	// recreate: a delayed old create could attach after a newer collection.
	if d.CreationAttempt {
		return errors.New("target_conflict")
	}
	if err = w.authority(); err != nil {
		return err
	}
	if err = w.update(func(d *workerData) error { d.CreationAttempt = true; return nil }); err != nil {
		return err
	}
	if err = w.authority(); err != nil {
		err = unsentError{err}
	} else {
		err = target.CreateOnce(ctx)
	}
	if err != nil {
		var retry *retryTargetError
		var unsent unsentError
		if errors.Is(err, ErrTargetRejected) || errors.As(err, &retry) || errors.As(err, &unsent) {
			if e := w.update(func(d *workerData) error {
				d.CreationAttempt = false
				if retry != nil {
					d.CreationRetryAt = retry.at
				}
				return nil
			}); e != nil {
				return ErrTargetOutcomeUncertain
			}
			return err
		}
		return ErrTargetOutcomeUncertain
	}
	if err = w.update(func(d *workerData) error { d.TargetCreated = true; return nil }); err != nil {
		return ErrTargetOutcomeUncertain
	}
	meta, err = target.Describe(ctx)
	if err != nil {
		return errors.New("target_unavailable")
	}
	if meta == nil {
		return errors.New("target_conflict")
	}
	return nil
}
func (w *Worker) prepareAndApply(ctx context.Context, a protocol.Approved, selection protocol.PostgresSelection, capture *postgrescdc.Capture, dsn string, target IntentTarget) error {
	d, err := w.data()
	if err != nil {
		return err
	}
	if d.Ready && d.ReadyRevision == a.Control.ResyncRevision {
		return nil
	}
	if err := w.phase("snapshotting", ""); err != nil {
		return err
	}
	receipt, err := w.source.CurrentCut(w.fingerprint)
	if err != nil {
		return errors.New("state_invalid")
	}
	pending, err := w.source.PreparedCut(w.fingerprint)
	if err != nil {
		return errors.New("state_invalid")
	}
	if receipt == nil {
		initialRevision := a.Control.ResyncRevision
		if pending != nil {
			initialRevision = pending.Revision
		}
		if a.Binding.Mode == "continuous" {
			if d.InitialRevision != nil {
				initialRevision = *d.InitialRevision
			} else {
				// Legacy initialized state predates revision-aware bootstrap and belongs to 0.
				retained, e := w.source.Load()
				if e != nil {
					return e
				}
				if retained != nil {
					initialRevision = 0
				}
				if err = w.update(func(d *workerData) error { d.InitialRevision = &initialRevision; return nil }); err != nil {
					return err
				}
			}
		}
		if a.Binding.Mode == "continuous" {
			if _, err = capture.Initialize(ctx, dsn); err != nil {
				return errors.New("source_setup_required")
			}
			if err = w.source.RecordInitialContinuous(w.fingerprint, initialRevision); err != nil {
				return err
			}
		} else {
			if pending == nil {
				snapshot, e := postgrescdc.ReadSnapshot(ctx, dsn, selection.Schema, selection.Table, selection.Columns)
				if e != nil {
					return errors.New("source_setup_required")
				}
				pending = &postgrescdc.Cut{Format: 1, Fingerprint: w.fingerprint, Revision: initialRevision, Snapshot: snapshot}
				if err = w.source.SavePreparedCut(*pending); err != nil {
					return err
				}
			}
			if err = w.source.PromotePreparedCut(w.fingerprint, initialRevision); err != nil {
				return err
			}
		}
		receipt, err = w.source.CurrentCut(w.fingerprint)
		if err != nil {
			return err
		}
	}
	revision := a.Control.ResyncRevision
	if receipt.Revision > revision {
		return errors.New("state_invalid")
	}
	var rows map[string]postgrescdc.Row
	var lsn uint64
	if receipt.Revision < revision {
		pending, err = w.source.PreparedCut(w.fingerprint)
		if err != nil {
			return err
		}
		if pending == nil || pending.Revision != revision {
			pending = &postgrescdc.Cut{Format: 1, Fingerprint: w.fingerprint, Revision: revision}
			if a.Binding.Mode == "continuous" {
				pending.Continuous, err = capture.Resnapshot(ctx, dsn)
			} else {
				pending.Snapshot, err = postgrescdc.ReadSnapshot(ctx, dsn, selection.Schema, selection.Table, selection.Columns)
				if err == nil {
					old, e := w.source.CurrentSnapshot(w.fingerprint)
					if e != nil || old == nil || old.Snapshot.RelationID != pending.Snapshot.RelationID || !reflect.DeepEqual(old.Snapshot.Columns, pending.Snapshot.Columns) {
						return errors.New("source_setup_required")
					}
				}
			}
			if err != nil {
				return errors.New("source_setup_required")
			}
			if err = w.source.SavePreparedCut(*pending); err != nil {
				return err
			}
		}
		if pending.Continuous != nil {
			if err = capture.ValidateCut(ctx, dsn, pending.Continuous); err != nil {
				return errors.New("source_setup_required")
			}
			rows = pending.Continuous.Rows
			lsn = pending.Continuous.LSN
		} else {
			rows = pending.Snapshot.Rows
		}
	} else if a.Binding.Mode == "continuous" {
		state, e := capture.Resume(ctx, dsn)
		if e != nil {
			return errors.New("source_setup_required")
		}
		rows = state.Rows
		lsn = state.LSN
	} else {
		cut, e := w.source.CurrentSnapshot(w.fingerprint)
		if e != nil || cut == nil {
			return errors.New("state_invalid")
		}
		rows = cut.Snapshot.Rows
	}
	if err := w.phase("applying", ""); err != nil {
		return err
	}
	if err = w.reconcile(ctx, target, revision, rows); err != nil {
		return err
	}
	if err = w.authority(); err != nil {
		return err
	}
	if receipt.Revision < revision {
		if err = w.source.PromotePreparedCut(w.fingerprint, revision); err != nil {
			return err
		}
	}
	return w.update(func(d *workerData) error {
		d.Ready = true
		d.ReadyRevision = revision
		d.Plan = nil
		d.Rows = uint64(len(rows))
		if lsn != 0 {
			d.Position = &protocol.Position{SourceType: "postgresql", Offset: pglogrepl.LSN(lsn).String()}
		}
		return nil
	})
}
func (w *Worker) reconcile(ctx context.Context, target IntentTarget, revision uint64, rows map[string]postgrescdc.Row) error {
	d, err := w.data()
	if err != nil {
		return err
	}
	if d.Plan == nil || d.Plan.Revision != revision {
		state, e := w.journal.Load()
		if e != nil {
			return e
		}
		deletes := []string{}
		for id := range state.ManagedIDs {
			if rows[id] == nil {
				deletes = append(deletes, id)
			}
		}
		sort.Strings(deletes)
		if err = w.update(func(d *workerData) error {
			d.Ready = false
			d.Plan = &syncPlan{Revision: revision, Deletes: deletes}
			return nil
		}); err != nil {
			return err
		}
		d, err = w.data()
		if err != nil {
			return err
		}
	}
	plan := *d.Plan
	ids := postgrescdc.SortedRowIDs(rows)
	if plan.DeleteCursor < 0 || plan.DeleteCursor > len(plan.Deletes) || plan.PutCursor < 0 || plan.PutCursor > len(ids) {
		return errors.New("state_invalid")
	}
	for index := plan.DeleteCursor; index < len(plan.Deletes); index++ {
		if err = w.delete(ctx, target, plan.Deletes[index]); err != nil {
			return err
		}
		if err = w.update(func(d *workerData) error { d.Plan.DeleteCursor = index + 1; return nil }); err != nil {
			return err
		}
	}
	for index := plan.PutCursor; index < len(ids); index++ {
		id := ids[index]
		if err = w.put(ctx, target, id, rows[id]); err != nil {
			return err
		}
		if err = w.update(func(d *workerData) error { d.Plan.PutCursor = index + 1; return nil }); err != nil {
			return err
		}
	}
	return nil
}
func (w *Worker) delete(ctx context.Context, target IntentTarget, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, _ := json.Marshal(map[string]any{"ids": []string{id}})
	if err := w.journal.Apply(ctx, target, "delete", raw); err != nil {
		return err
	}
	return nil
}
func (w *Worker) put(ctx context.Context, target IntentTarget, id string, row postgrescdc.Row) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	doc := make(map[string]json.RawMessage, len(row)+1)
	for key, value := range row {
		doc[key] = value
	}
	doc["id"], _ = json.Marshal(id)
	raw, err := json.Marshal(map[string]any{"docs": []map[string]json.RawMessage{doc}})
	if err != nil {
		return errors.New("state_invalid")
	}
	if err = w.journal.Apply(ctx, target, "upsert", raw); err != nil {
		return err
	}
	return nil
}
func (w *Worker) applyChanges(ctx context.Context, target IntentTarget, old, next map[string]postgrescdc.Row) error {
	for _, id := range postgrescdc.SortedRowIDs(old) {
		if next[id] == nil {
			if err := w.delete(ctx, target, id); err != nil {
				return err
			}
		}
	}
	for _, id := range postgrescdc.SortedRowIDs(next) {
		if old[id] == nil || !postgrescdc.EqualRows(old[id], next[id]) {
			if err := w.put(ctx, target, id, next[id]); err != nil {
				return err
			}
		}
	}
	return nil
}
