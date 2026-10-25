package postgrescdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

const MaxStateBytes = 32 << 20
const MaxRows = 10000
const MaxTransactionBytes = 8 << 20

type Row map[string]json.RawMessage
type Column struct {
	Name     string
	OID      uint32
	Key      bool
	Modifier int32
}
type State struct {
	Format      int            `json:"format"`
	Fingerprint string         `json:"fingerprint"`
	SystemID    string         `json:"system_id"`
	Timeline    int32          `json:"timeline"`
	Database    string         `json:"database"`
	RelationID  uint32         `json:"relation_id"`
	Columns     []Column       `json:"columns"`
	LSN         uint64         `json:"lsn"`
	Rows        map[string]Row `json:"rows"`
}

// A bbolt write transaction atomically fsyncs the complete row image and offset.
// Its exclusive file lock is our single-host execution ownership mechanism.
// Never copy an active database or put it on a network filesystem.
type Store struct{ db *bolt.DB }

type bootstrap struct {
	Fingerprint string
	SystemID    string
	Timeline    int32
	Database    string
}

func (s *Store) loadBootstrap() (*bootstrap, error) {
	var b *bootstrap
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("bootstrap"))
		if bucket == nil {
			return nil
		}
		if err := json.Unmarshal(bucket.Get([]byte("owner")), &b); err != nil || b == nil {
			return errors.New("corrupt bootstrap ownership; resync required")
		}
		return nil
	})
	return b, err
}
func (s *Store) saveBootstrap(b bootstrap) error {
	data, err := json.Marshal(b)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("bootstrap"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("owner"), data)
	})
}

func OpenStore(path string) (*Store, error) { return openStore(path, true) }

// OpenExistingStore is the managed runner restart path. Missing source row state
// cannot be reconstructed from a checkpoint alone and must never be auto-created.
func OpenExistingStore(path string) (*Store, error) { return openStore(path, false) }
func openStore(path string, create bool) (*Store, error) {
	options := &bolt.Options{Timeout: 100 * time.Millisecond}
	if !create {
		options.OpenFile = func(name string, flags int, mode os.FileMode) (*os.File, error) {
			f, err := os.OpenFile(name, flags&^os.O_CREATE, mode)
			if err != nil {
				return nil, err
			}
			info, err := f.Stat()
			if err != nil || info.Size() == 0 {
				_ = f.Close()
				return nil, errors.New("source state file is empty or unreadable")
			}
			return f, nil
		}
	}
	db, err := bolt.Open(path, 0600, options)
	if err != nil {
		return nil, fmt.Errorf("open regional state (exclusive ownership required): %w", err)
	}
	return &Store{db: db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Load() (*State, error) {
	var state *State
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("cdc"))
		if b == nil {
			return nil
		}
		v := b.Get([]byte("state"))
		if v == nil {
			return errors.New("regional state missing")
		}
		if len(v) > MaxStateBytes {
			return errors.New("regional state exceeds budget")
		}
		if err := json.Unmarshal(v, &state); err != nil {
			return errors.New("corrupt regional state; resync required")
		}
		if state == nil || state.Format != 1 || state.Rows == nil || state.LSN == 0 || len(state.Columns) == 0 || state.Fingerprint == "" || state.SystemID == "" {
			return errors.New("invalid regional state; resync required")
		}
		if len(state.Rows) > MaxRows {
			return errors.New("regional row budget exceeded")
		}
		for id, row := range state.Rows {
			actual, err := rowID(row, state.Columns)
			if err != nil || actual != id || len(row) != len(state.Columns) {
				return errors.New("corrupt regional row image; resync required")
			}
			for _, col := range state.Columns {
				if !supportedOID(col.OID) || !json.Valid(row[col.Name]) {
					return errors.New("invalid regional column; resync required")
				}
			}
		}
		return nil
	})
	return state, err
}
func (s *Store) Save(state *State) error {
	return s.save(state, nil)
}
func (s *Store) save(state *State, run *Generation) error {
	if len(state.Rows) > MaxRows {
		return errors.New("row budget exceeded")
	}
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(b) > MaxStateBytes {
		return errors.New("regional state budget exceeded")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		if run != nil {
			if err := managedAuthority(tx, *run); err != nil {
				return err
			}
		}
		bucket, err := tx.CreateBucketIfNotExists([]byte("cdc"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("state"), b); err != nil {
			return err
		}
		if tx.Bucket([]byte("bootstrap")) != nil {
			return tx.DeleteBucket([]byte("bootstrap"))
		}
		return nil
	})
}
func cloneRows(rows map[string]Row) map[string]Row {
	result := make(map[string]Row, len(rows))
	for k, v := range rows {
		result[k] = v
	}
	return result
}
func rowID(row Row, columns []Column) (string, error) {
	for _, c := range columns {
		if c.Key {
			v := row[c.Name]
			if len(v) == 0 || bytes.Equal(v, []byte("null")) {
				return "", errors.New("missing primary key")
			}
			var str string
			if json.Unmarshal(v, &str) == nil {
				if str == "" {
					return "", errors.New("empty primary key unsupported")
				}
				return str, nil
			}
			return string(v), nil
		}
	}
	return "", errors.New("primary key missing")
}
