package connections

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
	"github.com/lambdadb/lambdadb-migration/internal/statefile"
	bolt "go.etcd.io/bbolt"
)

type PendingInspection struct {
	Status    string `json:"status"`
	Sequence  uint64 `json:"sequence"`
	Operation string `json:"operation"`
	Digest    string `json:"digest"`
}

type Inspection struct {
	ApplicationPlan  *ApplicationPlan              `json:"application_plan,omitempty"`
	ConnectionID     string                        `json:"connection_id"`
	Region           string                        `json:"region"`
	Mode             string                        `json:"mode"`
	DesiredState     string                        `json:"desired_state"`
	DesiredRevision  uint64                        `json:"desired_revision"`
	Phase            string                        `json:"phase"`
	ReasonCode       string                        `json:"reason_code,omitempty"`
	TargetCreated    bool                          `json:"target_created"`
	CreationAttempt  bool                          `json:"creation_attempt"`
	AcceptedSequence uint64                        `json:"accepted_sequence"`
	ManagedIDCount   int                           `json:"managed_id_count"`
	Pending          *PendingInspection            `json:"pending,omitempty"`
	Source           *postgrescdc.SourceInspection `json:"source"`
	RecoveryRequired bool                          `json:"recovery_required"`
	ResumeAuthorized bool                          `json:"resume_authorized"`
	LocalStateOnly   bool                          `json:"local_state_only"`
	RemoteStateOnly  bool                          `json:"remote_state_only,omitempty"`
}

// InspectState requires a stopped worker and holds both files read-only together.
// It neither authorizes recovery nor reveals row content, IDs or secret references.
func InspectState(directory string) (*Inspection, error) {
	return inspectState(directory, nil)
}

// use runs while both original read-only locks are held.
func inspectState(directory string, use func(*Inspection) error) (*Inspection, error) {
	if directory == "" {
		return nil, errors.New("state directory required")
	}
	db, err := bolt.Open(filepath.Join(directory, "connection.db"), 0400,
		&bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond,
			OpenFile: func(name string, _ int, _ os.FileMode) (*os.File, error) { return os.Open(name) }})
	if err != nil {
		return nil, errors.New("journal inspection requires an existing valid file and a stopped worker")
	}
	defer db.Close()
	var fingerprint string
	if err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("connection"))
		if b == nil {
			return errors.New("connection journal is not initialized")
		}
		raw := b.Get([]byte("state"))
		var state State
		if len(raw) == 0 || len(raw) > MaxStateBytes || json.Unmarshal(raw, &state) != nil {
			return errors.New("connection journal is invalid")
		}
		fingerprint = state.Fingerprint
		return nil
	}); err != nil {
		return nil, err
	}
	j := &Journal{db: statefile.Guard(db), fingerprint: fingerprint}
	state, err := j.Load()
	if err != nil {
		return nil, err
	}
	d, err := (&Worker{journal: j, fingerprint: fingerprint}).data()
	if err != nil {
		return nil, err
	}
	o := protocol.Observation{ProtocolVersion: 1, ConnectionID: d.Approved.Binding.ConnectionID,
		DesiredRevision: d.Approved.Control.DesiredRevision, Sequence: 1, Phase: d.Phase, ReasonCode: d.Reason}
	if !o.Validate(o.ConnectionID) {
		return nil, errors.New("stored observation metadata is invalid")
	}
	source, err := postgrescdc.OpenReadOnlyStore(filepath.Join(directory, "source.db"))
	if err != nil {
		return nil, err
	}
	defer source.Close()
	metadata, err := source.Inspect(fingerprint, d.Approved.Binding.Mode)
	if err != nil {
		return nil, err
	}
	if d.Ready && (!metadata.CheckpointPresent || metadata.CurrentRevision == nil) {
		return nil, errors.New("ready connection has no retained source checkpoint")
	}
	out := &Inspection{ConnectionID: d.Approved.Binding.ConnectionID, Region: d.Approved.Binding.Region,
		Mode: d.Approved.Binding.Mode, DesiredState: d.Approved.Control.DesiredState,
		DesiredRevision: d.Approved.Control.DesiredRevision, Phase: d.Phase, ReasonCode: d.Reason,
		TargetCreated: d.TargetCreated, CreationAttempt: d.CreationAttempt, ApplicationPlan: state.Plan,
		AcceptedSequence: state.AcceptedSequence, ManagedIDCount: len(state.ManagedIDs), Source: metadata,
		RecoveryRequired: d.Reason == "target_outcome_uncertain" || (d.CreationAttempt && !d.TargetCreated),
		LocalStateOnly:   true}
	if state.Pending != nil {
		p := state.Pending
		out.Pending = &PendingInspection{Status: p.Status, Sequence: p.Sequence, Operation: p.Operation, Digest: p.Digest}
		out.RecoveryRequired = out.RecoveryRequired || p.Status == "sending" || p.Status == "rejected"
	}
	if use != nil {
		if err := use(out); err != nil {
			return nil, err
		}
	}
	return out, nil
}
