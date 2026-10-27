package postgrescdc

import (
	"errors"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	bolt "go.etcd.io/bbolt"
)

// OpenReadOnlyStore holds a shared lock. It never creates or updates a file.
func OpenReadOnlyStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0400, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond,
		OpenFile: func(name string, _ int, _ os.FileMode) (*os.File, error) { return os.Open(name) }})
	if err != nil {
		return nil, errors.New("source inspection requires an existing valid file and a stopped worker")
	}
	return &Store{db: db}, nil
}

type SourceInspection struct {
	Mode              string  `json:"mode"`
	CheckpointPresent bool    `json:"checkpoint_present"`
	LSN               string  `json:"lsn,omitempty"`
	Rows              int     `json:"rows"`
	CurrentRevision   *uint64 `json:"current_revision,omitempty"`
	PreparedRevision  *uint64 `json:"prepared_revision,omitempty"`
	PreparedRows      int     `json:"prepared_rows,omitempty"`
	BootstrapPresent  bool    `json:"bootstrap_present"`
}

// Inspect returns bounded metadata, never row values or credentials.
func (s *Store) Inspect(fingerprint, mode string) (*SourceInspection, error) {
	err := s.db.View(func(tx *bolt.Tx) error { _, err := sourceOwner(tx, fingerprint); return err })
	if err != nil {
		return nil, err
	}
	out := &SourceInspection{Mode: mode}
	current, err := s.CurrentCut(fingerprint)
	if err != nil {
		return nil, err
	}
	if current != nil {
		if current.Mode != mode {
			return nil, errors.New("source mode does not match the connection")
		}
		out.CurrentRevision = &current.Revision
	}
	if mode == "continuous" {
		state, err := s.Load()
		if err != nil {
			return nil, err
		}
		if state != nil {
			out.CheckpointPresent, out.LSN, out.Rows = true, pglogrepl.LSN(state.LSN).String(), len(state.Rows)
		}
	} else if mode == "snapshot" {
		cut, err := s.CurrentSnapshot(fingerprint)
		if err != nil {
			return nil, err
		}
		if cut != nil {
			out.CheckpointPresent, out.Rows = true, len(cut.Snapshot.Rows)
		}
	} else {
		return nil, errors.New("unsupported source inspection mode")
	}
	prepared, err := s.PreparedCut(fingerprint)
	if err != nil {
		return nil, err
	}
	if prepared != nil {
		if (mode == "continuous") != (prepared.Continuous != nil) {
			return nil, errors.New("prepared source mode does not match the connection")
		}
		out.PreparedRevision = &prepared.Revision
		if prepared.Continuous != nil {
			out.PreparedRows = len(prepared.Continuous.Rows)
		} else {
			out.PreparedRows = len(prepared.Snapshot.Rows)
		}
	}
	bootstrap, err := s.loadBootstrap()
	if err != nil {
		return nil, err
	}
	out.BootstrapPresent = bootstrap != nil
	return out, nil
}
