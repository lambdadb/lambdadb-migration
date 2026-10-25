// Package connections holds regional execution state shared by source adapters.
package connections

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

const MaxStateBytes = 32 << 20

var ErrPending = errors.New("a durable target intent must be recovered first")

type Intent struct {
	Sequence  uint64          `json:"sequence"`
	Operation string          `json:"operation"`
	Content   json.RawMessage `json:"content"`
	Digest    string          `json:"digest"`
}
type State struct {
	Format           int     `json:"format"`
	Fingerprint      string  `json:"fingerprint"`
	AcceptedSequence uint64  `json:"accepted_sequence"`
	Pending          *Intent `json:"pending,omitempty"`
	// Source and lifecycle state is opaque to this common journal. Credentials must
	// never be stored here; approved configuration contains regional references only.
	Data json.RawMessage `json:"data"`
}
type Journal struct {
	db          *bolt.DB
	fingerprint string
}

// OpenJournal requires an explicit one-time initialization. A normal restart
// never creates an absent state file, even if its parent volume is writable.
func OpenJournal(path, fingerprint string, initialize bool) (*Journal, error) {
	if fingerprint == "" {
		return nil, errors.New("connection fingerprint required")
	}
	options := &bolt.Options{Timeout: 100 * time.Millisecond, OpenFile: func(name string, flags int, mode os.FileMode) (*os.File, error) {
		if initialize {
			flags |= os.O_EXCL | os.O_CREATE
		} else {
			flags &^= os.O_CREATE
		}
		return os.OpenFile(name, flags, mode)
	}}
	db, err := bolt.Open(path, 0600, options)
	if err != nil {
		return nil, errors.New("regional connection state missing or exclusively owned")
	}
	j := &Journal{db: db, fingerprint: fingerprint}
	if initialize {
		err = j.db.Update(func(tx *bolt.Tx) error {
			b, e := tx.CreateBucket([]byte("connection"))
			if e != nil {
				return e
			}
			raw, _ := json.Marshal(State{Format: 1, Fingerprint: fingerprint, Data: json.RawMessage(`{}`)})
			return b.Put([]byte("state"), raw)
		})
	}
	if err == nil {
		_, err = j.Load()
	}
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return j, nil
}
func (j *Journal) Close() error { return j.db.Close() }
func (j *Journal) decode(tx *bolt.Tx) (*State, error) {
	b := tx.Bucket([]byte("connection"))
	if b == nil {
		return nil, errors.New("regional state is not initialized")
	}
	raw := b.Get([]byte("state"))
	if len(raw) == 0 || len(raw) > MaxStateBytes {
		return nil, errors.New("invalid regional state size")
	}
	var s State
	if json.Unmarshal(raw, &s) != nil || s.Format != 1 || s.Fingerprint != j.fingerprint || s.AcceptedSequence > math.MaxInt64 || !json.Valid(s.Data) {
		return nil, errors.New("regional state invalid or configuration changed")
	}
	if s.Pending != nil {
		p := s.Pending
		if p.Sequence != s.AcceptedSequence+1 || !validIntent(*p) {
			return nil, errors.New("invalid durable target intent")
		}
	}
	return &s, nil
}
func validIntent(p Intent) bool {
	return p.Sequence > 0 && p.Sequence <= math.MaxInt64 && (p.Operation == "upsert" || p.Operation == "delete") && json.Valid(p.Content) && p.Digest == intentDigest(p.Operation, p.Content)
}
func intentDigest(operation string, content []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(operation))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}
func (j *Journal) Load() (*State, error) {
	var s *State
	err := j.db.View(func(tx *bolt.Tx) error { var e error; s, e = j.decode(tx); return e })
	return s, err
}
func (j *Journal) write(tx *bolt.Tx, s *State) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(raw) > MaxStateBytes {
		return errors.New("regional state budget exceeded")
	}
	return tx.Bucket([]byte("connection")).Put([]byte("state"), raw)
}
func (j *Journal) SaveData(data json.RawMessage) error {
	if !json.Valid(data) {
		return errors.New("invalid regional source/lifecycle state")
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		s.Data = bytes.Clone(data)
		return j.write(tx, s)
	})
}

// Prepare durably assigns the sequence to the complete immutable operation before
// any network send. Caller content is compacted, never passed through float64.
func (j *Journal) Prepare(operation string, content json.RawMessage) (*Intent, error) {
	canonical, err := json.Marshal(content)
	if err != nil {
		return nil, errors.New("invalid target content")
	}
	var intent *Intent
	err = j.db.Update(func(tx *bolt.Tx) error {
		s, e := j.decode(tx)
		if e != nil {
			return e
		}
		if s.Pending != nil {
			return ErrPending
		}
		if s.AcceptedSequence >= math.MaxInt64 {
			return errors.New("target sequence exhausted")
		}
		p := Intent{Sequence: s.AcceptedSequence + 1, Operation: operation, Content: bytes.Clone(canonical)}
		p.Digest = intentDigest(operation, p.Content)
		if !validIntent(p) {
			return errors.New("invalid target operation")
		}
		s.Pending = &p
		intent = &p
		return j.write(tx, s)
	})
	return intent, err
}

// Accept clears only the exact acknowledged intent. A checkpoint cannot advance
// by accidentally acknowledging a later request or a changed payload.
func (j *Journal) Accept(intent Intent) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		if s.Pending == nil || s.Pending.Sequence != intent.Sequence || s.Pending.Digest != intent.Digest || s.Pending.Operation != intent.Operation || !bytes.Equal(s.Pending.Content, intent.Content) {
			return errors.New("target acceptance does not match pending intent")
		}
		s.AcceptedSequence = intent.Sequence
		s.Pending = nil
		return j.write(tx, s)
	})
}
