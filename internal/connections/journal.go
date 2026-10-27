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
const MaxManagedRows = 10000
const journalFormat = 3

var ErrPending = errors.New("a durable target intent is pending")
var ErrTargetOutcomeUncertain = errors.New("target_outcome_uncertain")
var ErrTargetRejected = errors.New("target_rejected")

type Intent struct {
	Status    string          `json:"status"`
	Sequence  uint64          `json:"sequence"`
	Operation string          `json:"operation"`
	Content   json.RawMessage `json:"content"`
	Digest    string          `json:"digest"`
}
type State struct {
	Format           int             `json:"format"`
	Fingerprint      string          `json:"fingerprint"`
	AcceptedSequence uint64          `json:"accepted_sequence"`
	Pending          *Intent         `json:"pending,omitempty"`
	ManagedIDs       map[string]bool `json:"managed_ids"`
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
		f, err := os.OpenFile(name, flags, mode)
		if err != nil {
			return nil, err
		}
		if !initialize {
			info, e := f.Stat()
			if e != nil || info.Size() == 0 {
				_ = f.Close()
				return nil, errors.New("regional state file is empty or unreadable")
			}
		}
		return f, nil
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
			raw, _ := json.Marshal(State{Format: journalFormat, Fingerprint: fingerprint, ManagedIDs: map[string]bool{}, Data: json.RawMessage(`{}`)})
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
	if json.Unmarshal(raw, &s) != nil || s.Format != journalFormat || s.ManagedIDs == nil || len(s.ManagedIDs) > MaxManagedRows || s.Fingerprint != j.fingerprint || s.AcceptedSequence > math.MaxInt64 || !json.Valid(s.Data) {
		return nil, errors.New("regional state invalid or configuration changed")
	}
	for id, present := range s.ManagedIDs {
		if id == "" || !present {
			return nil, errors.New("invalid managed document inventory")
		}
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
	_, err := intentID(p)
	return (p.Status == "prepared" || p.Status == "sending" || p.Status == "rejected") && p.Sequence > 0 && p.Sequence <= math.MaxInt64 && err == nil && p.Digest == intentDigest(p.Operation, p.Content)
}

// First delivery applies one document per intent. Keep the ownership inventory
// consistent with that exact content, independently of source checkpoint timing.
func intentID(p Intent) (string, error) {
	var body map[string]json.RawMessage
	if json.Unmarshal(p.Content, &body) != nil || len(body) != 1 {
		return "", errors.New("one target operation required")
	}
	var id string
	switch p.Operation {
	case "upsert":
		var docs []map[string]json.RawMessage
		if json.Unmarshal(body["docs"], &docs) != nil || len(docs) != 1 || json.Unmarshal(docs[0]["id"], &id) != nil {
			return "", errors.New("one upsert document with a string id required")
		}
	case "delete":
		var ids []string
		if json.Unmarshal(body["ids"], &ids) != nil || len(ids) != 1 {
			return "", errors.New("one deletion id required")
		}
		id = ids[0]
	default:
		return "", errors.New("unsupported target operation")
	}
	if id == "" {
		return "", errors.New("nonempty target id required")
	}
	return id, nil
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

// Prepare durably assigns a local journal sequence to the complete immutable operation before
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
		p := Intent{Status: "prepared", Sequence: s.AcceptedSequence + 1, Operation: operation, Content: bytes.Clone(canonical)}
		p.Digest = intentDigest(operation, p.Content)
		if !validIntent(p) {
			return errors.New("invalid target operation")
		}
		id, _ := intentID(p)
		if p.Operation == "upsert" && !s.ManagedIDs[id] && len(s.ManagedIDs) >= MaxManagedRows {
			return errors.New("managed document inventory budget exceeded")
		}
		s.Pending = &p
		intent = &p
		return j.write(tx, s)
	})
	return intent, err
}

// PendingError never sends a request. A sending intent can already have reached
// the target; neither restart nor a changed management command can retry it.
func (j *Journal) PendingError() error {
	s, err := j.Load()
	if err != nil {
		return err
	}
	if s.Pending != nil {
		switch s.Pending.Status {
		case "sending":
			return ErrTargetOutcomeUncertain
		case "rejected":
			return ErrTargetRejected
		}
	}
	return nil
}

func sameIntent(a, b Intent) bool {
	return a.Sequence == b.Sequence && a.Digest == b.Digest && a.Operation == b.Operation && bytes.Equal(a.Content, b.Content)
}

func (j *Journal) markSending(intent Intent) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		if s.Pending == nil || !sameIntent(*s.Pending, intent) || s.Pending.Status != "prepared" {
			return ErrPending
		}
		s.Pending.Status = "sending"
		return j.write(tx, s)
	})
}

func (j *Journal) reject(intent Intent) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		if s.Pending == nil || !sameIntent(*s.Pending, intent) || s.Pending.Status != "sending" {
			return ErrPending
		}
		s.Pending.Status = "rejected"
		return j.write(tx, s)
	})
}

// Accept clears only the exact acknowledged intent. A checkpoint cannot advance
// by accidentally acknowledging a later request or a changed payload.
func (j *Journal) Accept(intent Intent) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		if s.Pending == nil || s.Pending.Status != "sending" || !sameIntent(*s.Pending, intent) {
			return errors.New("target acceptance does not match pending intent")
		}
		id, err := intentID(*s.Pending)
		if err != nil {
			return err
		}
		if intent.Operation == "upsert" {
			s.ManagedIDs[id] = true
		} else {
			delete(s.ManagedIDs, id)
		}
		s.AcceptedSequence = intent.Sequence
		s.Pending = nil
		return j.write(tx, s)
	})
}

// UpdateData serializes lifecycle/cursor updates with observation sequencing.
// It does not replace a concurrently accepted target intent or ID inventory.
func (j *Journal) UpdateData(update func(json.RawMessage) (json.RawMessage, error)) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		data, err := update(bytes.Clone(s.Data))
		if err != nil {
			return err
		}
		if !json.Valid(data) {
			return errors.New("invalid regional lifecycle state")
		}
		s.Data = data
		return j.write(tx, s)
	})
}
