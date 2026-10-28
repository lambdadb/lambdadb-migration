package postgrescdc

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSnapshotCutPromotionRetainsOldDatasetUntilAccepted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.BindSource("binding", false); err == nil {
		t.Fatal("empty file adopted on restart")
	}
	if err = s.BindSource("binding", true); err != nil {
		t.Fatal(err)
	}
	cut := Cut{Format: 1, Fingerprint: "binding", Snapshot: &SnapshotData{RelationID: 1, Columns: []Column{{Name: "pk", OID: 23, Key: true}}, Rows: map[string]Row{"1": {"pk": []byte(`1`)}}}}
	if err = s.SavePreparedCut(cut); err != nil {
		t.Fatal(err)
	}
	if current, _ := s.CurrentCut("binding"); current != nil {
		t.Fatal("unaccepted cut made current")
	}
	if err = s.PromotePreparedCut("binding", 0); err != nil {
		t.Fatal(err)
	}
	cut.Revision = 2
	cut.Snapshot.Rows = map[string]Row{"2": {"pk": []byte(`2`)}}
	if err = s.SavePreparedCut(cut); err != nil {
		t.Fatal(err)
	}
	old, err := s.CurrentSnapshot("binding")
	if err != nil || old.Snapshot.Rows["1"] == nil {
		t.Fatal("prepared cut overwrote old dataset", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenExistingStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.BindSource("other", false); err == nil {
		t.Fatal("binding changed")
	}
	if err = s.PromotePreparedCut("binding", 2); err != nil {
		t.Fatal(err)
	}
	if err = s.PromotePreparedCut("binding", 2); err != nil {
		t.Fatal("promotion not idempotent", err)
	}
	if err = s.PromotePreparedCut("binding", 0); err == nil {
		t.Fatal("old promotion regressed current cut")
	}
	current, err := s.CurrentSnapshot("binding")
	if err != nil || current.Snapshot.Rows["2"] == nil || current.Snapshot.Rows["1"] != nil {
		t.Fatal("promoted dataset wrong", err)
	}
	if pending, _ := s.PreparedCut("binding"); pending != nil {
		t.Fatal("prepared state not cleared")
	}
}
func TestExplicitResnapshotRepairsOwnedSlotAndPromotesAfterTargetAcceptance(t *testing.T) {
	f, c := captureFixture(t)
	if err := c.Store.BindSource("binding", true); err != nil {
		t.Fatal(err)
	}
	baseline, err := c.Initialize(context.Background(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Store.RecordInitialContinuous("binding", 0); err != nil {
		t.Fatal(err)
	}
	execSQL(t, f.sql, "DELETE FROM "+f.c.Schema+".items WHERE id=1")
	if err = c.Disconnect(context.Background(), f.dsn); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Resume(context.Background(), f.dsn); err == nil {
		t.Fatal("lost slot silently recovered")
	}
	fresh, err := c.Resnapshot(context.Background(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Rows["1"] != nil || fresh.LSN <= baseline.LSN {
		t.Fatal("new cut incorrect")
	}
	if saved, _ := c.Store.Load(); saved.LSN != baseline.LSN || saved.Rows["1"] == nil {
		t.Fatal("resnapshot advanced applied state")
	}
	if err = c.Store.SavePreparedCut(Cut{Format: 1, Fingerprint: "binding", Revision: 2, Continuous: fresh}); err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateCut(context.Background(), f.dsn, fresh); err != nil {
		t.Fatal(err)
	}
	if err = c.Store.PromotePreparedCut("binding", 2); err != nil {
		t.Fatal(err)
	}
	if resumed, err := c.Resume(context.Background(), f.dsn); err != nil || resumed.LSN != fresh.LSN || resumed.Rows["1"] != nil {
		t.Fatal("new cut not resumable", err)
	}
}

func TestSourceProgressTracksSavePromotionAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	s, err := OpenNewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.BindSource("binding", true); err != nil {
		t.Fatal(err)
	}
	first := &State{Format: 1, Fingerprint: "source", SystemID: "system", LSN: 100, Columns: []Column{{Name: "id", OID: 23, Key: true}}, Rows: map[string]Row{"1": {"id": []byte(`1`)}}}
	if err = s.Save(first); err != nil {
		t.Fatal(err)
	}
	check := func(lsn, rows uint64) {
		t.Helper()
		p, e := s.Progress()
		if e != nil || p == nil || p.LSN != lsn || p.Rows != rows {
			t.Fatalf("progress: %+v %v", p, e)
		}
	}
	check(100, 1)
	if err = s.RecordInitialContinuous("binding", 3); err != nil {
		t.Fatal(err)
	}
	next := *first
	next.LSN = 200
	next.Rows = map[string]Row{}
	if err = s.SavePreparedCut(Cut{Format: 1, Fingerprint: "binding", Revision: 4, Continuous: &next}); err != nil {
		t.Fatal(err)
	}
	check(100, 1)
	if err = s.PromotePreparedCut("binding", 4); err != nil {
		t.Fatal(err)
	}
	check(200, 0)
	s.Close()
	s, err = OpenExistingStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check(200, 0)
	first.LSN = 300
	if err = s.Save(first); err != nil {
		t.Fatal(err)
	}
	check(300, 1)
	s.Close()
	if _, err = s.Progress(); err == nil {
		t.Fatal("cached progress bypassed failed store")
	}
}
