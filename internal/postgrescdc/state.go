package postgrescdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/statefile"
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
type Store struct {
	db *statefile.DB
	// Accessed only inside the serialized database transactions.
	progress *SourceProgress
}

type SourceProgress struct {
	LSN  uint64
	Rows uint64
}

// Progress avoids decoding the retained row image on every observation. The
// cache is reconstructed on open and invalidated by cut promotion.
func (s *Store) Progress() (*SourceProgress, error) {
	var result *SourceProgress
	err := s.db.View(func(tx *bolt.Tx) error {
		if s.progress == nil {
			if _, err := s.load(tx); err != nil {
				return err
			}
		}
		if s.progress != nil {
			copy := *s.progress
			result = &copy
		}
		return nil
	})
	return result, err
}

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

// OpenNewStore exclusively creates a newly registered connection's source file.
func OpenNewStore(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, errors.New("new source state file already exists or is unavailable")
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	return OpenStore(path)
}

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
	if create {
		if err = statefile.SyncParents(path); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return &Store{db: statefile.Guard(db)}, nil
}
func (s *Store) StorageError() error { return s.db.Err() }
func (s *Store) Close() error        { return s.db.Close() }
func (s *Store) Load() (*State, error) {
	var state *State
	err := s.db.View(func(tx *bolt.Tx) error { var err error; state, err = s.load(tx); return err })
	return state, err
}
func (s *Store) load(tx *bolt.Tx) (*State, error) {
	b := tx.Bucket([]byte("cdc"))
	if b == nil {
		return nil, nil
	}
	v := b.Get([]byte("state"))
	if v == nil {
		return nil, errors.New("regional state missing")
	}
	if len(v) > MaxStateBytes {
		return nil, errors.New("regional state exceeds budget")
	}
	var state *State
	if json.Unmarshal(v, &state) != nil {
		return nil, errors.New("corrupt regional state; resync required")
	}
	if err := validateState(state); err != nil {
		return nil, err
	}
	s.progress = &SourceProgress{LSN: state.LSN, Rows: uint64(len(state.Rows))}
	return state, nil
}
func validateState(state *State) error {
	if state == nil || state.Format != 1 || state.LSN == 0 || state.Fingerprint == "" || state.SystemID == "" {
		return errors.New("invalid regional state; resync required")
	}
	return validateRows(state.Columns, state.Rows)
}
func validateRows(columns []Column, rows map[string]Row) error {
	if rows == nil || len(columns) == 0 {
		return errors.New("invalid regional row image")
	}
	if len(rows) > MaxRows {
		return errors.New("regional row budget exceeded")
	}
	keys := 0
	names := map[string]bool{}
	for _, column := range columns {
		if names[column.Name] || column.Name == "" || !supportedOID(column.OID) {
			return errors.New("invalid regional columns")
		}
		names[column.Name] = true
		if column.Key {
			keys++
		}
	}
	if keys != 1 {
		return errors.New("single regional primary key required")
	}
	for id, row := range rows {
		actual, err := rowID(row, columns)
		if err != nil || actual != id || len(row) != len(columns) {
			return errors.New("corrupt regional row image; resync required")
		}
		for _, column := range columns {
			if !json.Valid(row[column.Name]) {
				return errors.New("invalid regional column; resync required")
			}
		}
	}
	return nil
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
		s.progress = &SourceProgress{LSN: state.LSN, Rows: uint64(len(state.Rows))}
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
