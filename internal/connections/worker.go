package connections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
)

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
	CreationAttempt bool               `json:"creation_attempt"`
	Ready           bool               `json:"ready"`
	ReadyRevision   uint64             `json:"ready_revision"`
	Plan            *syncPlan          `json:"plan,omitempty"`
	Position        *protocol.Position `json:"position,omitempty"`
	Rows            uint64             `json:"rows"`
}
type Worker struct {
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
func (w *Worker) authority() error {
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
	var observation protocol.Observation
	if err := w.update(func(d *workerData) error {
		if d.Sequence >= 1<<63-1 {
			return errors.New("observation sequence exhausted")
		}
		d.Sequence++
		observation = protocol.Observation{ProtocolVersion: 1, ConnectionID: d.Approved.Binding.ConnectionID, DesiredRevision: d.Approved.Control.DesiredRevision, Sequence: d.Sequence, Phase: d.Phase, ReasonCode: d.Reason, Position: d.Position, Rows: d.Rows}
		if d.Ready {
			observation.CompletedResyncRevision = d.ReadyRevision
			if d.Approved.Binding.Mode == "continuous" {
				state, e := w.source.Load()
				if e != nil {
					return e
				}
				if state != nil {
					observation.Position = &protocol.Position{SourceType: "postgresql", Offset: pglogrepl.LSN(state.LSN).String()}
					observation.Rows = uint64(len(state.Rows))
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return w.Control.Report(ctx, observation)
}

// Run owns one connection's retained files for its entire lifetime. Regional
// control is shared; neither source capture nor target batches call the backend.
func (w *Worker) Run(ctx context.Context) error {
	if w.Control == nil || w.Secrets == nil || w.StateDirectory == "" {
		return errors.New("invalid worker configuration")
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
		if approved.Control.DesiredRevision < d.Approved.Control.DesiredRevision {
			return errors.New("regional desired state regressed")
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
			finished = ""
			lastDesired = desiredKey
		}
		if cancel != nil && key != desiredKey {
			stop()
		}
		if cancel == nil {
			switch {
			case desiredKey == "expired":
				_ = w.phase("expired", "authority_expired")
			case d.Phase == "disconnected" && command.DesiredState == "disconnected":
				// The terminal receipt survives authority expiry and secret retirement.
			case command.DesiredState == "paused":
				_ = w.phase("paused", "")
			case d.Ready && d.ReadyRevision == command.ResyncRevision && d.Approved.Binding.Mode == "snapshot" && command.DesiredState == "running":
				_ = w.phase("completed", "")
			case finished != desiredKey && time.Now().After(nextAttempt):
				jobctx, jobcancel := context.WithCancel(ctx)
				cancel = jobcancel
				done = make(chan error, 1)
				key = desiredKey
				go func(a protocol.Approved) { done <- w.execute(jobctx, a) }(d.Approved)
			}
		}
		_ = w.report(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-done:
			cancel()
			cancel = nil
			done = nil
			if result == nil {
				finished = key
			} else {
				_ = w.phase("action_required", reasonCode(result))
				nextAttempt = time.Now().Add(5 * time.Second)
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
					if next.Control.DesiredRevision < d.Approved.Control.DesiredRevision || next.Control.ResyncRevision < d.Approved.Control.ResyncRevision || (d.Approved.Control.DesiredState == "disconnected" && next.Control.DesiredState != "disconnected") {
						return errors.New("regional desired state regressed")
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
	switch err.Error() {
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
		_ = w.phase("disconnecting", "")
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
		if d.CreationAttempt {
			if err = target.Disconnect(ctx); err != nil {
				return errors.New("target_unavailable")
			}
		}
		return w.phase("disconnected", "")
	}
	_ = w.phase("validating", "")
	if err = w.ensureTarget(ctx, target); err != nil {
		return err
	}
	guarded := guardedTarget{target, w.authority}
	if err = w.journal.RecoverPending(ctx, guarded); err != nil {
		return errors.New("target_unavailable")
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
	if err = w.prepareAndApply(ctx, a, selection, capture, dsn, guarded); err != nil {
		return err
	}
	if a.Binding.Mode == "snapshot" {
		return w.phase("completed", "")
	}
	if _, err = capture.Resume(ctx, dsn); err != nil {
		return errors.New("source_setup_required")
	}
	_ = w.phase("running", "")
	return capture.Stream(ctx, dsn, func(ctx context.Context, old, next *postgrescdc.State) error {
		return w.applyChanges(ctx, guarded, old.Rows, next.Rows)
	}, nil)
}

type guardedTarget struct {
	IntentTarget
	check func() error
}

func (t guardedTarget) Send(ctx context.Context, p Intent) error {
	if err := t.check(); err != nil {
		return err
	}
	return t.IntentTarget.Send(ctx, p)
}
