package postgrescdc

import (
	"encoding/json"
	"errors"
	"math"

	bolt "go.etcd.io/bbolt"
)

// Cut is a complete source dataset prepared regionally before target application.
// The old current checkpoint is retained until target reconciliation succeeds.
type Cut struct {
	Format      int           `json:"format"`
	Fingerprint string        `json:"fingerprint"`
	Revision    uint64        `json:"revision"`
	Continuous  *State        `json:"continuous,omitempty"`
	Snapshot    *SnapshotData `json:"snapshot,omitempty"`
}
type CutReceipt struct {
	Revision uint64 `json:"revision"`
	Mode     string `json:"mode"`
}

var sourceBucket = []byte("connection-source")

// BindSource is called with initialize only for an explicitly created, empty
// source file. A restart validates ownership instead of adopting an empty file.
func (s *Store) BindSource(fingerprint string, initialize bool) error {
	if fingerprint == "" {
		return errors.New("source binding fingerprint required")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(sourceBucket)
		if b == nil {
			if !initialize || tx.Bucket([]byte("cdc")) != nil || tx.Bucket([]byte("bootstrap")) != nil {
				return errors.New("regional source ownership missing")
			}
			var err error
			b, err = tx.CreateBucket(sourceBucket)
			if err != nil {
				return err
			}
			return b.Put([]byte("owner"), []byte(fingerprint))
		}
		if string(b.Get([]byte("owner"))) != fingerprint {
			return errors.New("regional source binding changed")
		}
		return nil
	})
}
func sourceOwner(tx *bolt.Tx, fingerprint string) (*bolt.Bucket, error) {
	b := tx.Bucket(sourceBucket)
	if b == nil || string(b.Get([]byte("owner"))) != fingerprint {
		return nil, errors.New("regional source binding missing or changed")
	}
	return b, nil
}
func (c *Cut) validate(fingerprint string) error {
	if c == nil || c.Format != 1 || c.Fingerprint != fingerprint || c.Revision > math.MaxInt64 || (c.Continuous == nil) == (c.Snapshot == nil) {
		return errors.New("invalid prepared source cut")
	}
	if c.Continuous != nil {
		return validateState(c.Continuous)
	}
	if c.Snapshot.RelationID == 0 {
		return errors.New("invalid snapshot relation")
	}
	return validateRows(c.Snapshot.Columns, c.Snapshot.Rows)
}
func decodeCut(raw []byte, fingerprint string) (*Cut, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > MaxStateBytes {
		return nil, errors.New("source cut budget exceeded")
	}
	var cut Cut
	if json.Unmarshal(raw, &cut) != nil {
		return nil, errors.New("invalid stored source cut")
	}
	if err := cut.validate(fingerprint); err != nil {
		return nil, err
	}
	return &cut, nil
}
func (s *Store) SavePreparedCut(cut Cut) error {
	if err := cut.validate(cut.Fingerprint); err != nil {
		return err
	}
	raw, err := json.Marshal(cut)
	if err != nil {
		return err
	}
	if len(raw) > MaxStateBytes {
		return errors.New("source cut budget exceeded")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := sourceOwner(tx, cut.Fingerprint)
		if err != nil {
			return err
		}
		if current := b.Get([]byte("current")); current != nil {
			var receipt CutReceipt
			if json.Unmarshal(current, &receipt) != nil || cut.Revision <= receipt.Revision {
				return errors.New("prepared source cut is not newer than current")
			}
		}
		return b.Put([]byte("prepared"), raw)
	})
}
func (s *Store) PreparedCut(fingerprint string) (*Cut, error) {
	var cut *Cut
	err := s.db.View(func(tx *bolt.Tx) error {
		b, e := sourceOwner(tx, fingerprint)
		if e != nil {
			return e
		}
		cut, e = decodeCut(b.Get([]byte("prepared")), fingerprint)
		return e
	})
	return cut, err
}
func (s *Store) CurrentCut(fingerprint string) (*CutReceipt, error) {
	var receipt *CutReceipt
	err := s.db.View(func(tx *bolt.Tx) error {
		b, e := sourceOwner(tx, fingerprint)
		if e != nil {
			return e
		}
		raw := b.Get([]byte("current"))
		if raw == nil {
			return nil
		}
		if json.Unmarshal(raw, &receipt) != nil || receipt == nil || (receipt.Mode != "continuous" && receipt.Mode != "snapshot") || receipt.Revision > math.MaxInt64 {
			return errors.New("invalid current source cut")
		}
		return nil
	})
	return receipt, err
}
func (s *Store) CurrentSnapshot(fingerprint string) (*Cut, error) {
	var cut *Cut
	err := s.db.View(func(tx *bolt.Tx) error {
		b, e := sourceOwner(tx, fingerprint)
		if e != nil {
			return e
		}
		cut, e = decodeCut(b.Get([]byte("snapshot")), fingerprint)
		return e
	})
	return cut, err
}
func (s *Store) RecordInitialContinuous(fingerprint string, revision uint64) error {
	if revision > math.MaxInt64 {
		return errors.New("invalid initial source revision")
	}
	state, err := s.Load()
	if err != nil {
		return err
	}
	if state == nil {
		return errors.New("initial continuous snapshot missing")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b, e := sourceOwner(tx, fingerprint)
		if e != nil {
			return e
		}
		if raw := b.Get([]byte("current")); raw != nil {
			var r CutReceipt
			if json.Unmarshal(raw, &r) != nil || r.Revision != revision || r.Mode != "continuous" {
				return errors.New("source cut already advanced")
			}
			return nil
		}
		raw, _ := json.Marshal(CutReceipt{Revision: revision, Mode: "continuous"})
		return b.Put([]byte("current"), raw)
	})
}

// PromotePreparedCut atomically replaces source row/position state and its cut
// receipt. Retry after a crash can distinguish promotion from a new snapshot.
func (s *Store) PromotePreparedCut(fingerprint string, revision uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, err := sourceOwner(tx, fingerprint)
		if err != nil {
			return err
		}
		if raw := b.Get([]byte("current")); raw != nil {
			var r CutReceipt
			if json.Unmarshal(raw, &r) != nil {
				return errors.New("invalid current source cut")
			}
			if r.Revision == revision {
				return nil
			}
			if r.Revision > revision {
				return errors.New("obsolete source cut promotion")
			}
		}
		cut, err := decodeCut(b.Get([]byte("prepared")), fingerprint)
		if err != nil {
			return err
		}
		if cut == nil || cut.Revision != revision {
			return errors.New("matching prepared source cut required")
		}
		mode := "snapshot"
		if cut.Continuous != nil {
			s.progress = nil
			mode = "continuous"
			state, err := json.Marshal(cut.Continuous)
			if err != nil {
				return err
			}
			bucket, e := tx.CreateBucketIfNotExists([]byte("cdc"))
			if e != nil {
				return e
			}
			if e = bucket.Put([]byte("state"), state); e != nil {
				return e
			}
			if tx.Bucket([]byte("bootstrap")) != nil {
				if e = tx.DeleteBucket([]byte("bootstrap")); e != nil {
					return e
				}
			}
		} else {
			raw, err := json.Marshal(cut)
			if err != nil {
				return err
			}
			if err = b.Put([]byte("snapshot"), raw); err != nil {
				return err
			}
		}
		receipt, _ := json.Marshal(CutReceipt{Revision: revision, Mode: mode})
		if err = b.Put([]byte("current"), receipt); err != nil {
			return err
		}
		return b.Delete([]byte("prepared"))
	})
}
