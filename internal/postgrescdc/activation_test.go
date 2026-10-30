package postgrescdc

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	bolt "go.etcd.io/bbolt"
)

func readyGeneration(t *testing.T, s *Store, c Config, lsn uint64) Generation {
	t.Helper()
	run, err := s.beginGeneration(c)
	if err != nil {
		t.Fatal(err)
	}
	run, err = s.nameGeneration(run, "generation_"+pglogrepl.LSN(lsn).String())
	if err != nil {
		t.Fatal(err)
	}
	state := decoderState()
	state.Fingerprint = c.fingerprint()
	state.LSN = lsn
	if err = s.Save(state); err != nil {
		t.Fatal(err)
	}
	digest, err := rowsDigest(state.Rows)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.recordVerified(run, lsn, digest); err != nil {
		t.Fatal(err)
	}
	return run
}
func TestGenerationSelectionCAS(t *testing.T) {
	c := testConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	s := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	run, err := s.beginGeneration(c)
	if err != nil {
		t.Fatal(err)
	}
	run, err = s.nameGeneration(run, "unverified")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.activate(run, 0); err == nil {
		t.Fatal("unverified generation selected")
	}
	first := readyGeneration(t, s, c, 100)
	selected, err := s.activate(first, 0)
	if err != nil {
		t.Fatal(err)
	}
	if retry, err := s.activate(first, 0); err != nil || *retry != *selected {
		t.Fatal("selection retry is not idempotent")
	}
	if err = s.recordVerified(first, 200, "different"); err == nil {
		t.Fatal("selected proof was rewritten")
	}
	second := readyGeneration(t, s, c, 200)
	if _, err = s.activate(second, 0); err == nil {
		t.Fatal("obsolete expected selection accepted")
	}
	if err = s.recordVerified(first, 100, selected.Digest); err == nil {
		t.Fatal("stale verification accepted")
	}
	if _, err = s.activate(first, 0); err == nil {
		t.Fatal("stale worker selected its generation")
	}
	if _, err = s.activate(second, first.Epoch); err != nil {
		t.Fatal(err)
	}
	if _, err = s.activate(first, 0); err == nil {
		t.Fatal("old selection regressed successor")
	}
	e, err := s.Execution()
	if err != nil || e.Active.Epoch != second.Epoch || e.Active.BarrierLSN != 200 {
		t.Fatal("wrong active selection")
	}
}

type rejectedVerification struct{ Target }

func (r rejectedVerification) Verify(context.Context, string, map[string]Row) error {
	return errors.New("injected full verification failure")
}

func TestPostgresVerificationFailureDoesNotSelect(t *testing.T) {
	f := newFixture(t)
	s := openTestStore(t, f.path)
	r := start(t, &Worker{Config: f.c, Store: s, Target: rejectedVerification{Target: &memoryTarget{}}}, f.dsn)
	select {
	case err := <-r.done:
		if err == nil || err.Error() != "injected full verification failure" {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("verification failure timeout")
	}
	r.cancel()
	e, err := s.Execution()
	if err != nil || e.Active != nil || e.Owner.VerifiedLSN != 0 {
		t.Fatal("failed verification selected a generation")
	}
}
func TestGenerationProofMustMatchCheckpoint(t *testing.T) {
	c := testConfig()
	_ = c.Validate()
	for _, cause := range []string{"state_changed", "progress_regression", "authority_expired"} {
		t.Run(cause, func(t *testing.T) {
			s := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
			first := readyGeneration(t, s, c, 100)
			if _, err := s.activate(first, 0); err != nil {
				t.Fatal(err)
			}
			second := readyGeneration(t, s, c, 200)
			switch cause {
			case "state_changed":
				state := decoderState()
				state.Fingerprint = c.fingerprint()
				state.LSN = 201
				if err := s.Save(state); err != nil {
					t.Fatal(err)
				}
			case "progress_regression":
				second = readyGeneration(t, s, c, 90)
			case "authority_expired":
				if err := s.db.Update(func(tx *bolt.Tx) error {
					e, err := loadExecution(tx)
					if err != nil {
						return err
					}
					e.Owner.ExpiresAt = time.Now().Add(-time.Second)
					return saveExecution(tx, e)
				}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.activate(second, first.Epoch); err == nil {
				t.Fatal("invalid selection accepted")
			}
			e, err := s.Execution()
			if err != nil || e.Active.Epoch != first.Epoch {
				t.Fatal("rejected activation changed active selection")
			}
		})
	}
}
func TestCatchupBarrierRequiresTransactionalCommit(t *testing.T) {
	d := &transactionDecoder{state: decoderState(), barrierToken: "candidate", barrierMessageLSN: 150}
	_, err := d.consume(&pglogrepl.LogicalDecodingMessage{LSN: 150, Prefix: barrierPrefix, Content: []byte("candidate"), Transactional: false}, 1)
	if err == nil {
		t.Fatal("nontransactional barrier accepted")
	}
	for _, m := range []pglogrepl.Message{
		&pglogrepl.BeginMessage{FinalLSN: 190},
		&pglogrepl.LogicalDecodingMessage{LSN: 150, Prefix: barrierPrefix, Content: []byte("candidate"), Transactional: true},
	} {
		if _, err = d.consume(m, 1); err != nil {
			t.Fatal(err)
		}
	}
	if d.committedBarrier {
		t.Fatal("uncommitted barrier considered applied")
	}
	state, err := d.consume(&pglogrepl.CommitMessage{CommitLSN: 190, TransactionEndLSN: 200}, 1)
	if err != nil || state.LSN != 200 || !d.committedBarrier {
		t.Fatal("barrier commit was not recognized")
	}
}
