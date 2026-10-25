package postgrescdc

import (
	"context"
	"errors"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
	"testing"
	"time"
)

func approvedTest(c Config) Approved {
	return Approved{Binding: c, Control: Command{ProtocolVersion: 1, BindingID: c.BindingID, DesiredRevision: 1, DesiredState: "running", AuthorityExpiresAt: c.AuthorityExpiresAt}}
}
func TestManagedRenewPauseAndEpochFencing(t *testing.T) {
	c := testConfig()
	_ = c.Validate()
	s := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	a := approvedTest(c)
	if err := s.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	run, err := s.begin(c, true)
	if err != nil {
		t.Fatal(err)
	}
	run, err = s.nameGeneration(run, "generation_one")
	if err != nil {
		t.Fatal(err)
	}
	state := decoderState()
	state.Fingerprint = c.fingerprint()
	if err = s.save(state, &run); err != nil {
		t.Fatal(err)
	}
	a.Control.DesiredRevision = 2
	a.Control.AuthorityExpiresAt = a.Control.AuthorityExpiresAt.Add(time.Hour)
	if err = s.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	e, _ := s.Execution()
	if e.Owner.Epoch != run.Epoch || e.Owner.Name != run.Name || !e.Owner.ExpiresAt.Equal(a.Control.AuthorityExpiresAt) {
		t.Fatal("renewal changed execution")
	}
	if err = s.checkManaged(run); err != nil {
		t.Fatal(err)
	}
	if err = s.AcceptControl(approvedTest(c), c.BindingID); !errors.Is(err, ErrObsoleteControl) {
		t.Fatal("stale command accepted")
	}
	conflict := a
	conflict.Control.DesiredState = "paused"
	if err = s.AcceptControl(conflict, c.BindingID); !errors.Is(err, ErrConflictingControl) {
		t.Fatal("same revision conflict accepted")
	}
	if err = s.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal("duplicate failed", err)
	}
	a.Control.DesiredRevision++
	a.Control.DesiredState = "paused"
	if err = s.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if s.checkManaged(run) == nil || s.save(state, &run) == nil {
		t.Fatal("paused epoch can checkpoint")
	}
	digest, _ := rowsDigest(state.Rows)
	if s.recordVerified(run, state.LSN, digest) == nil {
		t.Fatal("paused epoch can verify")
	}
	if _, err = s.begin(c, true); err == nil {
		t.Fatal("paused binding can begin")
	}
	a.Control.DesiredRevision++
	a.Control.DesiredState = "running"
	if err = s.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if s.checkManaged(run) == nil {
		t.Fatal("resume revived old epoch")
	}
	next, err := s.begin(c, true)
	if err != nil || next.Epoch != run.Epoch+1 {
		t.Fatal("resume epoch", err)
	}
	loaded, _ := s.Load()
	if !equalJSON(loaded, state) {
		t.Fatal("control changed row checkpoint")
	}
}
func TestLegacyFingerprintUpgradeAndControlRestart(t *testing.T) {
	c := testConfig()
	_ = c.Validate()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	state := decoderState()
	state.Fingerprint = c.legacyFingerprint()
	if err = s.Save(state); err != nil {
		t.Fatal(err)
	}
	if err = s.saveBootstrap(bootstrap{Fingerprint: c.legacyFingerprint(), SystemID: "source"}); err != nil {
		t.Fatal(err)
	}
	if err = s.db.Update(func(tx *bolt.Tx) error {
		return saveExecution(tx, &Execution{Format: 1, Owner: Generation{Epoch: 1, Fingerprint: c.legacyFingerprint(), ExpiresAt: c.AuthorityExpiresAt}})
	}); err != nil {
		t.Fatal(err)
	}
	if err = s.AcceptControl(approvedTest(c), c.BindingID); err != nil {
		t.Fatal(err)
	}
	state.Fingerprint = c.fingerprint()
	loaded, _ := s.Load()
	if !equalJSON(loaded, state) {
		t.Fatal("legacy checkpoint changed")
	}
	execution, _ := s.Execution()
	if execution.Owner.Epoch != 1 || execution.Owner.Fingerprint != c.fingerprint() {
		t.Fatal("legacy execution not upgraded")
	}
	owner, _ := s.loadBootstrap()
	if owner.Fingerprint != c.fingerprint() {
		t.Fatal("legacy owner not upgraded")
	}
	seq, _ := s.observationSequence()
	_ = s.Close()
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cached, err := s.Approved()
	if err != nil || cached.Control.DesiredRevision != 1 {
		t.Fatal("lost cached approval", err)
	}
	next, _ := s.observationSequence()
	if next != seq+1 {
		t.Fatal("observation sequence regressed")
	}
	changed := *cached
	changed.Binding.Table = "other"
	if s.AcceptControl(changed, c.BindingID) == nil {
		t.Fatal("mapping drift accepted")
	}
	expired := *cached
	expired.Control.DesiredRevision++
	expired.Control.AuthorityExpiresAt = time.Now().Add(-time.Second)
	if err = s.AcceptControl(expired, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.begin(c, true); err == nil {
		t.Fatal("expired control can execute")
	}
}

func TestStandaloneCannotBypassManagedPause(t *testing.T) {
	c := testConfig()
	store := openTestStore(t, filepath.Join(t.TempDir(), "managed.db"))
	a := approvedTest(c)
	a.Control.DesiredState = "paused"
	if err := store.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	worker := Worker{Config: c, Store: store, Target: &memoryTarget{}}
	if err := worker.Run(context.Background(), "invalid-dsn"); err == nil || err.Error() != "managed regional state requires the control supervisor" {
		t.Fatalf("standalone bypass: %v", err)
	}
}
