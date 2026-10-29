package postgrescdc

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestPreparedTransactionCannotBeReplacedOrAdvanceFeedbackEarly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	s, err := OpenNewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	old := &State{Format: 1, Fingerprint: "source", SystemID: "system", LSN: 100,
		Columns: []Column{{Name: "pk", OID: 20, Key: true}}, Rows: map[string]Row{"1": {"pk": json.RawMessage(`1`)}}}
	next := *old
	next.LSN = 200
	next.Rows = map[string]Row{"2": {"pk": json.RawMessage(`2`)}}
	if err = s.Save(old); err != nil {
		t.Fatal(err)
	}
	if err = s.PrepareTransaction(old, &next); err != nil {
		t.Fatal(err)
	}
	progress, err := s.Progress()
	if err != nil || progress.LSN != 100 {
		t.Fatal("unaccepted transaction advanced progress", err)
	}
	changed := next
	changed.LSN++
	if s.PrepareTransaction(old, &changed) == nil || s.Save(&changed) == nil || s.Save(old) == nil {
		t.Fatal("retained transaction replaced by a different source checkpoint")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenExistingStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.PreparedTransaction()
	if err != nil || p == nil || p.FromLSN != 100 || p.Next.LSN != 200 || p.Next.Rows["2"] == nil {
		t.Fatal("prepared commit not recoverable", err)
	}
	if err = s.Save(p.Next); err != nil {
		t.Fatal(err)
	}
	p, err = s.PreparedTransaction()
	if err != nil || p != nil {
		t.Fatal("checkpoint and pending transaction did not advance atomically", err)
	}
	progress, err = s.Progress()
	if err != nil || progress.LSN != 200 {
		t.Fatal("committed source position not visible", err)
	}
}

func TestPreparedTransactionMatchesSerializedCheckpoint(t *testing.T) {
	s, err := OpenNewStore(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	old := &State{Format: 1, Fingerprint: "source", SystemID: "system", LSN: 100,
		Columns: []Column{{Name: "pk", OID: 20, Key: true}, {Name: "payload", OID: 3802}},
		Rows:    map[string]Row{"1": {"pk": json.RawMessage(`1`), "payload": json.RawMessage(`{"keep": "initial", "large": 9007199254740993}`)}}}
	if err = s.Save(old); err != nil {
		t.Fatal(err)
	}
	// PostgreSQL supplies spaced JSON; Save compacts RawMessage values while
	// the decoder keeps its in-memory predecessor for the next commit.
	loaded, err := s.Load()
	if err != nil || string(loaded.Rows["1"]["payload"]) == string(old.Rows["1"]["payload"]) {
		t.Fatal("fixture did not exercise the serialization boundary", err)
	}
	next := *old
	next.LSN = 200
	next.Rows = map[string]Row{}
	changed := *old
	changed.Rows = map[string]Row{"1": {"pk": json.RawMessage(`1`), "payload": json.RawMessage(`{"keep":"changed","large":9007199254740993}`)}}
	if s.PrepareTransaction(&changed, &next) == nil {
		t.Fatal("a different predecessor row image was accepted")
	}
	if err = s.PrepareTransaction(old, &next); err != nil {
		t.Fatal("equivalent serialized predecessor was rejected", err)
	}
	if err = s.Save(&next); err != nil {
		t.Fatal(err)
	}
}
