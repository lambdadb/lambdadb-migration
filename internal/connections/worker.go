package connections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
	"github.com/lambdadb/lambdadb-migration/internal/statefile"
)

// RemoteState is optional for standalone execution and required by managed S3
// task templates. Objects is injected only by isolated tests or an embedding host.
type RemoteState struct {
	Bucket  string
	Prefix  string
	StoreID string
	Epoch   uint64
	Objects statefile.Objects
}

type syncPlan struct {
	Revision     uint64   `json:"revision"`
	Deletes      []string `json:"deletes"`
	DeleteCursor int      `json:"delete_cursor"`
	PutCursor    int      `json:"put_cursor"`
}
type workerData struct {
	Approved        protocol.Approved  `json:"approved"`
	Sequence        uint64             `json:"observation_sequence"`
	Phase           string             `json:"phase"`
	Reason          string             `json:"reason,omitempty"`
	TargetCreated   bool               `json:"target_created"`
	CreationAttempt bool               `json:"creation_attempt"`
	CreationRetryAt time.Time          `json:"creation_retry_at,omitempty"`
	Ready           bool               `json:"ready"`
	InitialRevision *uint64            `json:"initial_revision,omitempty"`
	ReadyRevision   uint64             `json:"ready_revision"`
	Plan            *syncPlan          `json:"plan,omitempty"`
	Position        *protocol.Position `json:"position,omitempty"`
	Rows            uint64             `json:"rows"`
}
type Worker struct {
	RemoteState         *RemoteState
	Control             *Control
	Secrets             SecretResolver
	StateDirectory      string
	Region              string
	Initialize          bool
	AllowInsecureSource bool
	PollInterval        time.Duration
	Transport           http.RoundTripper
	journal             *Journal
	source              *postgrescdc.Store
	fingerprint         string
	lastReport          time.Time
	lastObservation     protocol.Observation
}

func (w *Worker) data() (*workerData, error) {
	state, err := w.journal.Load()
	if err != nil {
		return nil, err
	}
	var data workerData
	if json.Unmarshal(state.Data, &data) != nil || data.Approved.Validate(data.Approved.Binding.Region) != nil || protocol.Fingerprint(data.Approved.Binding) != w.fingerprint {
		return nil, errors.New("regional worker state invalid")
	}
	return &data, nil
}
func (w *Worker) update(fn func(*workerData) error) error {
	return w.journal.UpdateData(func(raw json.RawMessage) (json.RawMessage, error) {
		var d workerData
		if json.Unmarshal(raw, &d) != nil {
			return nil, errors.New("regional worker state invalid")
		}
		if err := fn(&d); err != nil {
			return nil, err
		}
		return json.Marshal(d)
	})
}
func (w *Worker) phase(phase, reason string) error {
	return w.update(func(d *workerData) error { d.Phase = phase; d.Reason = reason; return nil })
}
func (w *Worker) storageError() error {
	if err := w.journal.db.Err(); err != nil {
		return err
	}
	return w.source.StorageError()
}
func (w *Worker) authority() error {
	if err := w.storageError(); err != nil {
		return err
	}
	d, err := w.data()
	if err != nil {
		return err
	}
	if !time.Now().Before(d.Approved.Control.AuthorityExpiresAt) {
		return errors.New("authority_expired")
	}
	return nil
}
func (w *Worker) report(ctx context.Context) error {
	if err := w.storageError(); err != nil {
		return err
	}
	d, err := w.data()
	if err != nil {
		return err
	}
	observation := protocol.Observation{ProtocolVersion: 1, ConnectionID: d.Approved.Binding.ConnectionID, DesiredRevision: d.Approved.Control.DesiredRevision, Phase: d.Phase, ReasonCode: d.Reason, Position: d.Position, Rows: d.Rows}
	if d.Ready {
		observation.CompletedResyncRevision = d.ReadyRevision
		if d.Approved.Binding.Mode == "continuous" {
			state, err := w.source.Progress()
			if err != nil {
				return err
			}
			if state != nil {
				observation.Position = &protocol.Position{SourceType: "postgresql", Offset: pglogrepl.LSN(state.LSN).String()}
				observation.Rows = state.Rows
			}
		}
	}
	// Lifecycle changes report immediately; unchanged heartbeats remain below the
	// regional service's 15-second staleness threshold without a per-second fsync.
	if time.Since(w.lastReport) < 5*time.Second && reflect.DeepEqual(observation, w.lastObservation) {
		return nil
	}
	if err := w.update(func(d *workerData) error {
		if d.Sequence >= 1<<63-1 {
			return errors.New("observation sequence exhausted")
		}
		d.Sequence++
		observation.Sequence = d.Sequence
		return nil
	}); err != nil {
		return err
	}
	err = w.Control.Report(ctx, observation)
	observation.Sequence = 0
	w.lastObservation = observation
	w.lastReport = time.Now()
	return err
}

// Run owns one connection's retained files for its entire lifetime. Regional
// control is shared; neither source capture nor target batches call the backend.
func (w *Worker) Run(ctx context.Context) error {
	if w.Control == nil || w.Secrets == nil || w.StateDirectory == "" {
		return errors.New("invalid worker configuration")
	}
	if _, err := os.Lstat(filepath.Join(w.StateDirectory, restoredStateMarker)); err == nil {
		return errors.New("restored archive is inspection-only; worker resume is forbidden")
	} else if !os.IsNotExist(err) {
		return errors.New("regional state directory unavailable")
	}
	approved, err := w.Control.Desired(ctx)
	if err != nil {
		return err
	}
	if w.Region != "" && w.Region != approved.Binding.Region {
		return errors.New("worker must run in the target region")
	}
	w.Region = approved.Binding.Region
	w.fingerprint = protocol.Fingerprint(approved.Binding)
	observed, err := w.Control.Observed(ctx)
	if err != nil {
		return err
	}
	if w.Initialize && observed != nil {
		return errors.New("initialization forbidden for an already observed connection")
	}
	var replica *statefile.Replica
	if w.RemoteState != nil {
		cacheRoot := w.StateDirectory
		lock, e := statefile.LockCache(cacheRoot)
		if e != nil {
			return e
		}
		defer lock.Close()
		remote := w.RemoteState
		objects := remote.Objects
		if objects == nil {
			objects, e = statefile.NewS3Objects(ctx, w.Region, remote.Bucket)
			if e != nil {
				return e
			}
		}
		replica, e = statefile.OpenReplica(ctx, objects, remote.Prefix, statefile.Binding{StoreID: remote.StoreID, Region: w.Region, ConnectionID: approved.Binding.ConnectionID, Fingerprint: w.fingerprint}, remote.Epoch)
		if e != nil {
			return e
		}
		if w.Initialize {
			w.StateDirectory, e = os.MkdirTemp(cacheRoot, "cache-")
		} else {
			var files map[string]statefile.Image
			files, e = replica.Recover()
			if e == nil {
				w.StateDirectory, e = statefile.Materialize(cacheRoot, files)
			}
		}
		if e != nil {
			return e
		}
	}
	if w.Initialize {
		if err = os.MkdirAll(w.StateDirectory, 0700); err != nil {
			return errors.New("regional state directory unavailable")
		}
	}
	w.journal, err = OpenJournal(filepath.Join(w.StateDirectory, "connection.db"), w.fingerprint, w.Initialize)
	if err != nil {
		return err
	}
	defer w.journal.Close()
	if w.Initialize {
		w.source, err = postgrescdc.OpenNewStore(filepath.Join(w.StateDirectory, "source.db"))
	} else {
		w.source, err = postgrescdc.OpenExistingStore(filepath.Join(w.StateDirectory, "source.db"))
	}
	if err != nil {
		return errors.New("regional source state missing or exclusively owned")
	}
	defer w.source.Close()
	if err = w.source.BindSource(w.fingerprint, w.Initialize); err != nil {
		return err
	}
	if w.Initialize {
		raw, _ := json.Marshal(workerData{Approved: approved, Phase: "initializing"})
		if err = w.journal.SaveData(raw); err != nil {
			return err
		}
	}
	if replica != nil {
		if w.Initialize {
			journal, e := statefile.Export(w.journal.db)
			if e != nil {
				return e
			}
			source, e := w.source.Export()
			if e != nil {
				return e
			}
			if err = replica.Initialize(map[string]statefile.Image{"connection": journal, "source": source}); err != nil {
				return err
			}
		}
		if err = w.journal.db.Attach(replica, "connection"); err != nil {
			return err
		}
		if err = w.source.AttachReplica(replica); err != nil {
			return err
		}
	}
	if w.Initialize {
		// The first atomic observation claims initialization. A second host cannot
		// initialize the same ID concurrently with another sequence-one claimant.
		if err = w.report(ctx); err != nil {
			return err
		}
	} else {
		if _, err = w.data(); err != nil {
			return err
		}
	}
	if err = w.update(func(d *workerData) error {
		if err := validateControl(d.Approved, approved); err != nil {
			return err
		}
		d.Approved = approved
		if observed != nil && observed.Observation.Sequence > d.Sequence {
			d.Sequence = observed.Observation.Sequence
		}
		return nil
	}); err != nil {
		return err
	}
	interval := w.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var cancel context.CancelFunc
	var done chan error
	var key, finished, lastDesired string
	nextAttempt := time.Time{}
	retryDelay := 5 * time.Second
	stop := func() {
		if cancel != nil {
			cancel()
			<-done
			cancel = nil
			done = nil
			key = ""
		}
	}
	defer stop()
	for {
		d, e := w.data()
		if e != nil {
			return e
		}
		command := d.Approved.Control
		desiredKey := command.DesiredState + "/" + stringRevision(command.ResyncRevision)
		if !time.Now().Before(command.AuthorityExpiresAt) && !(d.Phase == "disconnected" && command.DesiredState == "disconnected") && !(d.Ready && d.ReadyRevision == command.ResyncRevision && d.Approved.Binding.Mode == "snapshot" && command.DesiredState == "running") {
			desiredKey = "expired"
		}
		if desiredKey != lastDesired {
			nextAttempt = time.Time{}
			retryDelay = 5 * time.Second
			finished = ""
			lastDesired = desiredKey
		}
		if cancel != nil && key != desiredKey {
			stop()
		}
		if cancel == nil {
			// Cancellation may have left an in-flight request. Reload after stop;
			// no desired command can clear evidence of an uncertain target write.
			d, e = w.data()
			if e != nil {
				return e
			}
			blocked := w.journal.PendingError()
			if d.Reason == "target_outcome_uncertain" || (d.CreationAttempt && !d.TargetCreated) {
				blocked = ErrTargetOutcomeUncertain
			}
			switch {
			case blocked != nil:
				if !errors.Is(blocked, ErrTargetOutcomeUncertain) && !errors.Is(blocked, ErrTargetRejected) {
					return blocked
				}
				if err := w.phase("action_required", reasonCode(blocked)); err != nil {
					return err
				}
			case desiredKey == "expired":
				if err := w.phase("expired", "authority_expired"); err != nil {
					return err
				}
			case d.Phase == "disconnected" && command.DesiredState == "disconnected":
				// The terminal receipt survives authority expiry and secret retirement.
			case command.DesiredState == "paused":
				if err := w.phase("paused", ""); err != nil {
					return err
				}
			case d.Ready && d.ReadyRevision == command.ResyncRevision && d.Approved.Binding.Mode == "snapshot" && command.DesiredState == "running":
				if err := w.phase("completed", ""); err != nil {
					return err
				}
			case finished != desiredKey && time.Now().After(nextAttempt):
				jobctx, jobcancel := context.WithCancel(ctx)
				cancel = jobcancel
				done = make(chan error, 1)
				key = desiredKey
				go func(a protocol.Approved) { done <- w.execute(jobctx, a) }(d.Approved)
			}
		}
		if err := w.report(ctx); err != nil {
			// Regional HTTP failures do not interrupt capture under cached authority.
			// Storage failures must never retry or report through the failed handle.
			if fatal := w.storageError(); fatal != nil {
				return fatal
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-done:
			cancel()
			cancel = nil
			done = nil
			if fatal := w.storageError(); fatal != nil {
				return fatal
			}
			if result == nil {
				finished = key
				retryDelay = 5 * time.Second
			} else {
				if err := w.phase("action_required", reasonCode(result)); err != nil {
					return err
				}
				nextAttempt = time.Now().Add(retryDelay)
				var retry *retryTargetError
				if errors.As(result, &retry) {
					if retry.at.After(nextAttempt) {
						nextAttempt = retry.at
					}
					retryDelay = min(retryDelay*2, time.Minute)
				}
			}
			key = ""
		case <-ticker.C:
			next, e := w.Control.Desired(ctx)
			if e == nil {
				if protocol.Fingerprint(next.Binding) != w.fingerprint {
					stop()
					return errors.New("immutable connection binding changed")
				}
				if e = w.update(func(d *workerData) error {
					if err := validateControl(d.Approved, next); err != nil {
						return err
					}
					d.Approved = next
					return nil
				}); e != nil {
					stop()
					return e
				}
			}
		}
	}
}
func reasonCode(err error) string {
	if errors.Is(err, ErrTargetOutcomeUncertain) {
		return "target_outcome_uncertain"
	}
	if errors.Is(err, ErrTargetRejected) {
		return "target_rejected"
	}
	switch err.Error() {
	case "target_rate_limited":
		return "target_unavailable"
	case "authority_expired":
		return "authority_expired"
	case "target_conflict":
		return "target_conflict"
	case "target_unavailable":
		return "target_unavailable"
	case "state_invalid":
		return "state_invalid"
	case "source_setup_required":
		return "source_setup_required"
	}
	return "operation_failed"
}
func (w *Worker) execute(ctx context.Context, a protocol.Approved) error {
	if err := w.journal.PendingError(); err != nil {
		return err
	}
	if err := w.authority(); err != nil {
		return err
	}
	d, err := w.data()
	if err != nil {
		return err
	}
	// A completed disconnection remains valid after the backend retires secrets.
	if d.Phase == "disconnected" && a.Control.DesiredState == "disconnected" {
		return nil
	}
	targetKey, err := w.Secrets.Resolve(ctx, a.Binding.Region, a.Binding.Target.CredentialRef)
	if err != nil {
		return errors.New("target_unavailable")
	}
	target, err := NewAPITarget(a.Binding, targetKey)
	if err != nil {
		return errors.New("target_conflict")
	}
	target.client.Transport = w.Transport
	var selection protocol.PostgresSelection
	if protocol.Decode(bytes.NewReader(a.Binding.Source.Configuration), &selection) != nil {
		return errors.New("state_invalid")
	}
	capture := &postgrescdc.Capture{Selection: postgrescdc.CaptureSelection{ConnectionID: a.Binding.ConnectionID, Region: a.Binding.Region, Schema: selection.Schema, Table: selection.Table, Publication: selection.Publication, Columns: selection.Columns}, Store: w.source, Authorize: w.authority}
	d, err = w.data()
	if err != nil {
		return err
	}
	if a.Control.DesiredState == "disconnected" {
		if err := w.phase("disconnecting", ""); err != nil {
			return err
		}
		if a.Binding.Mode == "continuous" {
			sourceSecret, e := w.Secrets.Resolve(ctx, a.Binding.Region, a.Binding.Source.CredentialRef)
			if e != nil {
				return errors.New("source_setup_required")
			}
			dsn, e := SourceDSN(sourceSecret, w.AllowInsecureSource)
			if e != nil {
				return errors.New("source_setup_required")
			}
			if e = capture.Disconnect(ctx, dsn); e != nil {
				return errors.New("source_setup_required")
			}
		}
		return w.phase("disconnected", "")
	}
	if err := w.phase("validating", ""); err != nil {
		return err
	}
	if err = w.ensureTarget(ctx, target); err != nil {
		return err
	}
	guarded := guardedTarget{target, w.authority}
	if err = w.journal.RecoverPending(ctx, guarded); err != nil {
		return err
	}
	d, err = w.data()
	if err != nil {
		return err
	}
	if d.Ready && d.ReadyRevision == a.Control.ResyncRevision && a.Binding.Mode == "snapshot" {
		return w.phase("completed", "")
	}
	secret, err := w.Secrets.Resolve(ctx, a.Binding.Region, a.Binding.Source.CredentialRef)
	if err != nil {
		return errors.New("source_setup_required")
	}
	dsn, err := SourceDSN(secret, w.AllowInsecureSource)
	if err != nil {
		return errors.New("source_setup_required")
	}
	if err = w.recoverTransaction(ctx, capture, dsn, guarded); err != nil {
		return err
	}
	d, err = w.data()
	if err != nil {
		return err
	}
	if d.Plan != nil && d.Plan.Revision < a.Control.ResyncRevision {
		previous := a
		previous.Control.ResyncRevision = d.Plan.Revision
		if err = w.prepareAndApply(ctx, previous, selection, capture, dsn, guarded); err != nil {
			return err
		}
	}
	if err = w.prepareAndApply(ctx, a, selection, capture, dsn, guarded); err != nil {
		return err
	}
	if a.Binding.Mode == "snapshot" {
		return w.phase("completed", "")
	}
	if _, err = capture.Resume(ctx, dsn); err != nil {
		return errors.New("source_setup_required")
	}
	if err := w.phase("running", ""); err != nil {
		return err
	}
	return capture.Stream(ctx, dsn, func(ctx context.Context, old, next *postgrescdc.State) error {
		if err := w.source.PrepareTransaction(old, next); err != nil {
			return err
		}
		return w.applyChanges(ctx, guarded, old, next)
	}, nil)
}

// Finish a retained commit before a new stream or resync can replace its plan.
// Save clears the prepared state atomically with the new row image and position.
func (w *Worker) recoverTransaction(ctx context.Context, capture *postgrescdc.Capture, dsn string, target IntentTarget) error {
	p, err := w.source.PreparedTransaction()
	if err != nil || p == nil {
		return err
	}
	if err = capture.ValidateCut(ctx, dsn, p.Next); err != nil {
		return errors.New("source_setup_required")
	}
	old, err := w.source.Load()
	if err != nil {
		return err
	}
	if err = w.applyChanges(ctx, target, old, p.Next); err != nil {
		return err
	}
	if err = w.authority(); err != nil {
		return err
	}
	return w.source.Save(p.Next)
}

type guardedTarget struct {
	IntentTarget
	check func() error
}

func (t guardedTarget) authorize() error { return t.check() }

func (t guardedTarget) Send(ctx context.Context, p Intent) error {
	if err := t.check(); err != nil {
		return unsentError{err}
	}
	return t.IntentTarget.Send(ctx, p)
}

// Startup and polling must enforce the same immutable command history.
func validateControl(previous, next protocol.Approved) error {
	if protocol.Fingerprint(previous.Binding) != protocol.Fingerprint(next.Binding) {
		return errors.New("immutable connection binding changed")
	}
	before, after := previous.Control, next.Control
	if after.DesiredRevision < before.DesiredRevision || after.ResyncRevision < before.ResyncRevision || (before.DesiredState == "disconnected" && after.DesiredState != "disconnected") {
		return errors.New("regional desired state regressed")
	}
	if after.DesiredRevision == before.DesiredRevision && (after.ProtocolVersion != before.ProtocolVersion || after.ConnectionID != before.ConnectionID || after.DesiredState != before.DesiredState || after.ResyncRevision != before.ResyncRevision || !after.AuthorityExpiresAt.Equal(before.AuthorityExpiresAt)) {
		return errors.New("conflicting regional control revision")
	}
	return nil
}
