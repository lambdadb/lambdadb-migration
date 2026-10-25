package postgrescdc

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Generation is regional execution authority, separate from source progress.
// Starting a newer epoch invalidates every older verification/selection request.
type Generation struct {
	Epoch               uint64    `json:"epoch"`
	Name                string    `json:"name,omitempty"`
	Fingerprint         string    `json:"fingerprint"`
	ExpiresAt           time.Time `json:"expires_at"`
	ExpectedActiveEpoch uint64    `json:"expected_active_epoch"`
	VerifiedLSN         uint64    `json:"verified_lsn,omitempty"`
	VerifiedDigest      string    `json:"verified_digest,omitempty"`
}

// Selection records a locally verified generation. It is not a serving alias,
// a liveness signal, or proof of query-mode visibility.
type Selection struct {
	Epoch      uint64 `json:"epoch"`
	Generation string `json:"generation"`
	BarrierLSN uint64 `json:"barrier_lsn"`
	Digest     string `json:"digest"`
}
type Execution struct {
	Format int        `json:"format"`
	Owner  Generation `json:"owner"`
	Active *Selection `json:"active,omitempty"`
}

func loadExecution(tx *bolt.Tx) (*Execution, error) {
	b := tx.Bucket([]byte("execution"))
	if b == nil {
		return &Execution{Format: 1}, nil
	}
	var e Execution
	if json.Unmarshal(b.Get([]byte("state")), &e) != nil || e.Format != 1 || e.Owner.Epoch == 0 || e.Owner.Fingerprint == "" {
		return nil, errors.New("corrupt regional execution state")
	}
	if e.Owner.ExpectedActiveEpoch >= e.Owner.Epoch || (e.Owner.VerifiedLSN == 0) != (e.Owner.VerifiedDigest == "") || (e.Owner.VerifiedLSN != 0 && e.Owner.Name == "") {
		return nil, errors.New("invalid regional verification state")
	}
	if e.Active != nil && (e.Active.Epoch == 0 || e.Active.Epoch > e.Owner.Epoch || e.Active.Generation == "" || e.Active.BarrierLSN == 0 || e.Active.Digest == "") {
		return nil, errors.New("invalid regional selection state")
	}
	if (e.Active == nil && e.Owner.ExpectedActiveEpoch != 0) || (e.Active != nil && e.Active.Epoch != e.Owner.Epoch && e.Active.Epoch != e.Owner.ExpectedActiveEpoch) {
		return nil, errors.New("regional selection predecessor changed")
	}
	if e.Active != nil && e.Active.Epoch == e.Owner.Epoch && (e.Active.Generation != e.Owner.Name || e.Active.BarrierLSN != e.Owner.VerifiedLSN || e.Active.Digest != e.Owner.VerifiedDigest) {
		return nil, errors.New("selected generation has no matching verification proof")
	}
	return &e, nil
}
func saveExecution(tx *bolt.Tx, e *Execution) error {
	b, err := tx.CreateBucketIfNotExists([]byte("execution"))
	if err != nil {
		return err
	}
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return b.Put([]byte("state"), data)
}
func (s *Store) Execution() (*Execution, error) {
	var e *Execution
	err := s.db.View(func(tx *bolt.Tx) error { var err error; e, err = loadExecution(tx); return err })
	return e, err
}
func (s *Store) beginGeneration(c Config) (Generation, error) {
	return s.begin(c, false)
}
func (s *Store) begin(c Config, managed bool) (Generation, error) {
	var run Generation
	err := s.db.Update(func(tx *bolt.Tx) error {
		e, err := loadExecution(tx)
		if err != nil {
			return err
		}
		if e.Owner.Epoch == math.MaxUint64 || (e.Owner.Epoch != 0 && e.Owner.Fingerprint != c.fingerprint()) {
			return errors.New("execution identity changed or epoch exhausted")
		}
		run = Generation{Epoch: e.Owner.Epoch + 1, Fingerprint: c.fingerprint(), ExpiresAt: c.AuthorityExpiresAt}
		if managed {
			a, err := approved(tx)
			if err != nil {
				return err
			}
			if a == nil || a.Binding.fingerprint() != run.Fingerprint || a.Control.DesiredState != "running" || !time.Now().Before(a.Control.AuthorityExpiresAt) {
				return errors.New("managed execution authority unavailable")
			}
			run.ExpiresAt = a.Control.AuthorityExpiresAt
		}
		if e.Active != nil {
			run.ExpectedActiveEpoch = e.Active.Epoch
		}
		e.Owner = run
		return saveExecution(tx, e)
	})
	return run, err
}
func owned(e *Execution, run Generation) error {
	if e.Owner.Epoch != run.Epoch || e.Owner.Fingerprint != run.Fingerprint || e.Owner.Name != run.Name || !time.Now().Before(e.Owner.ExpiresAt) {
		return errors.New("stale or expired generation authority")
	}
	return nil
}
func (s *Store) nameGeneration(run Generation, name string) (Generation, error) {
	err := s.db.Update(func(tx *bolt.Tx) error {
		e, err := loadExecution(tx)
		if err != nil {
			return err
		}
		if err = owned(e, run); err != nil {
			return err
		}
		if name == "" || e.Owner.Name != "" {
			return errors.New("generation name already assigned or empty")
		}
		e.Owner.Name = name
		run = e.Owner
		return saveExecution(tx, e)
	})
	return run, err
}

func rowsDigest(rows map[string]Row) (string, error) {
	docs := make(map[string]any, len(rows))
	for id, row := range rows {
		docs[id] = document(id, row)
	}
	data, err := json.Marshal(docs)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
func checkpointMatches(tx *bolt.Tx, run Generation, lsn uint64, digest string) error {
	b := tx.Bucket([]byte("cdc"))
	if b == nil {
		return errors.New("no recoverable baseline")
	}
	var state State
	if json.Unmarshal(b.Get([]byte("state")), &state) != nil || state.Format != 1 || state.Fingerprint != run.Fingerprint || state.LSN != lsn || state.Rows == nil {
		return errors.New("verification checkpoint changed")
	}
	actual, err := rowsDigest(state.Rows)
	if err != nil || actual != digest {
		return errors.New("verification row state changed")
	}
	return nil
}
func (s *Store) recordVerified(run Generation, lsn uint64, digest string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		e, err := loadExecution(tx)
		if err != nil {
			return err
		}
		if err = owned(e, run); err != nil {
			return err
		}
		if run.Name == "" || lsn == 0 || digest == "" {
			return errors.New("generation has no verification proof")
		}
		if e.Owner.VerifiedLSN != 0 {
			if e.Owner.VerifiedLSN == lsn && e.Owner.VerifiedDigest == digest {
				return nil
			}
			return errors.New("generation verification proof is immutable")
		}
		if err = checkpointMatches(tx, run, lsn, digest); err != nil {
			return err
		}
		e.Owner.VerifiedLSN = lsn
		e.Owner.VerifiedDigest = digest
		return saveExecution(tx, e)
	})
}

// activate performs a compare-and-swap against both execution epoch and the
// expected previous selection. Its proof must describe the exact saved barrier.
func (s *Store) activate(run Generation, expected uint64) (*Selection, error) {
	var selected *Selection
	err := s.db.Update(func(tx *bolt.Tx) error {
		e, err := loadExecution(tx)
		if err != nil {
			return err
		}
		if err = owned(e, run); err != nil {
			return err
		}
		if expected != e.Owner.ExpectedActiveEpoch || e.Owner.VerifiedLSN == 0 || e.Owner.VerifiedDigest == "" {
			return errors.New("unverified generation or obsolete selection request")
		}
		proof := &Selection{Epoch: run.Epoch, Generation: run.Name, BarrierLSN: e.Owner.VerifiedLSN, Digest: e.Owner.VerifiedDigest}
		if e.Active != nil && *e.Active == *proof {
			selected = e.Active
			return nil
		}
		current := uint64(0)
		if e.Active != nil {
			current = e.Active.Epoch
			if proof.BarrierLSN < e.Active.BarrierLSN {
				return errors.New("selection would regress source progress")
			}
		}
		if current != expected {
			return errors.New("previous selection changed")
		}
		if err = checkpointMatches(tx, run, proof.BarrierLSN, proof.Digest); err != nil {
			return err
		}
		e.Active = proof
		selected = proof
		if err = saveExecution(tx, e); err != nil {
			return err
		}
		processBoundary("activation_write")
		return nil
	})
	return selected, err
}
