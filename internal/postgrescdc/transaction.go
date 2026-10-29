package postgrescdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"

	bolt "go.etcd.io/bbolt"
)

// PreparedTransaction retains a decoded commit until its entire target plan and
// source checkpoint have been accepted. It is never PostgreSQL feedback itself.
type PreparedTransaction struct {
	FromLSN uint64 `json:"from_lsn"`
	Next    *State `json:"next"`
}

var transactionBucket = []byte("connection-transaction")

func sameSource(a, b *State) bool {
	return a != nil && b != nil && a.Fingerprint == b.Fingerprint && a.SystemID == b.SystemID && a.Timeline == b.Timeline && a.Database == b.Database && a.RelationID == b.RelationID && reflect.DeepEqual(a.Columns, b.Columns)
}

func (s *Store) PrepareTransaction(old, next *State) error {
	if validateState(old) != nil || validateState(next) != nil || !sameSource(old, next) || next.LSN <= old.LSN {
		return errors.New("invalid source transaction")
	}
	raw, err := json.Marshal(next)
	if err != nil || len(raw) > MaxStateBytes {
		return errors.New("source transaction state budget exceeded")
	}
	from, _ := json.Marshal(old.LSN)
	return s.db.Update(func(tx *bolt.Tx) error {
		current, err := s.load(tx)
		if err != nil || !reflect.DeepEqual(current, old) {
			return errors.New("source transaction checkpoint changed")
		}
		b := tx.Bucket(transactionBucket)
		if b != nil {
			if !bytes.Equal(b.Get([]byte("from")), from) || !bytes.Equal(b.Get([]byte("next")), raw) {
				return errors.New("a different source transaction is already prepared")
			}
			return nil
		}
		b, err = tx.CreateBucket(transactionBucket)
		if err != nil {
			return err
		}
		if err = b.Put([]byte("from"), from); err != nil {
			return err
		}
		return b.Put([]byte("next"), raw)
	})
}

func (s *Store) PreparedTransaction() (*PreparedTransaction, error) {
	var prepared *PreparedTransaction
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(transactionBucket)
		if b == nil {
			return nil
		}
		var p PreparedTransaction
		if len(b.Get([]byte("next"))) > MaxStateBytes || json.Unmarshal(b.Get([]byte("from")), &p.FromLSN) != nil || json.Unmarshal(b.Get([]byte("next")), &p.Next) != nil || validateState(p.Next) != nil {
			return errors.New("invalid prepared source transaction")
		}
		current, err := s.load(tx)
		if err != nil || !sameSource(current, p.Next) || p.FromLSN != current.LSN || p.Next.LSN <= p.FromLSN {
			return errors.New("prepared source transaction does not match checkpoint")
		}
		prepared = &p
		return nil
	})
	return prepared, err
}

func completeTransaction(tx *bolt.Tx, raw []byte) error {
	b := tx.Bucket(transactionBucket)
	if b == nil {
		return nil
	}
	if !bytes.Equal(b.Get([]byte("next")), raw) {
		return errors.New("source checkpoint does not match prepared transaction")
	}
	return tx.DeleteBucket(transactionBucket)
}
