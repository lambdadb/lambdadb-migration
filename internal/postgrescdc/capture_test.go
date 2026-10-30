package postgrescdc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

func captureFixture(t *testing.T) (*fixture, *Capture) {
	t.Helper()
	f := projectedFixture(t)
	store := openTestStore(t, f.path)
	capture := &Capture{Selection: CaptureSelection{ConnectionID: f.c.Schema, Region: "ap-northeast-2", Schema: f.c.Schema, Table: "items", Publication: f.c.Publication, Columns: []string{"id", "title", "body"}}, Store: store}
	config, _ := capture.Selection.config()
	t.Cleanup(func() {
		_, _ = f.sql.Exec(context.Background(), `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active`, config.Slot)
	})
	return f, capture
}
func runCapture(t *testing.T, c *Capture, dsn string, apply func(context.Context, *State, *State) error) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- c.Stream(ctx, dsn, apply, func() { close(ready) }) }()
	select {
	case <-ready:
	case err := <-done:
		cancel()
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("capture did not become ready")
	}
	return cancel, done
}
func stopCapture(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("capture did not stop")
	}
}
func awaitCapture(t *testing.T, c *Capture, predicate func(*State) bool) *State {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s, err := c.Store.Load()
		if err != nil {
			t.Fatal(err)
		}
		if s != nil && predicate(s) {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("capture checkpoint did not advance")
	return nil
}
func TestCaptureRetainsBaselineAndReplaysOnlyCommittedChangesOnRestart(t *testing.T) {
	f, c := captureFixture(t)
	baseline, err := c.Initialize(context.Background(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if len(baseline.Rows) != 2 || string(baseline.Rows["1"]["title"]) != `"baseline"` {
		t.Fatal("invalid initial cut")
	}
	cancel, done := runCapture(t, c, f.dsn, func(context.Context, *State, *State) error { return nil })
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET title='changed' WHERE id=1;DELETE FROM "+f.c.Schema+".items WHERE id=2;INSERT INTO "+f.c.Schema+".items(id,title,body) VALUES(3,'inserted','new')")
	beforeRestart := awaitCapture(t, c, func(s *State) bool {
		return s.Rows["3"] != nil && s.Rows["2"] == nil && string(s.Rows["1"]["title"]) == `"changed"`
	})
	stopCapture(t, cancel, done)
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=10,title='moved',metadata='{\"excluded\":true}' WHERE id=1")
	resumed, err := c.Resume(context.Background(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.LSN != beforeRestart.LSN || !equalJSON(resumed.Rows, beforeRestart.Rows) {
		t.Fatal("restart scanned a new baseline instead of retaining its checkpoint")
	}
	cancel, done = runCapture(t, c, f.dsn, func(context.Context, *State, *State) error { return nil })
	after := awaitCapture(t, c, func(s *State) bool { return s.Rows["10"] != nil && s.Rows["1"] == nil })
	stopCapture(t, cancel, done)
	if after.LSN <= beforeRestart.LSN || string(after.Rows["10"]["body"]) != string(baseline.Rows["1"]["body"]) || after.Rows["10"]["metadata"] != nil {
		t.Fatal("PK/TOAST/projection reconstruction regressed")
	}
	if err = c.Disconnect(context.Background(), f.dsn); err != nil {
		t.Fatal(err)
	}
	if err = c.Disconnect(context.Background(), f.dsn); err != nil {
		t.Fatal("disconnect not idempotent", err)
	}
	var publications int
	if err = f.sql.QueryRow(context.Background(), `SELECT count(*) FROM pg_publication WHERE pubname=$1`, f.c.Publication).Scan(&publications); err != nil || publications != 1 {
		t.Fatal("disconnect removed the customer publication")
	}
}
func TestCaptureDoesNotCheckpointOrAcknowledgeRejectedTargetApplication(t *testing.T) {
	f, c := captureFixture(t)
	baseline, err := c.Initialize(context.Background(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	rejected := errors.New("target acceptance not proven")
	cancel, done := runCapture(t, c, f.dsn, func(context.Context, *State, *State) error { return rejected })
	defer cancel()
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET title='pending' WHERE id=1")
	select {
	case err = <-done:
		if !errors.Is(err, rejected) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("capture did not reject uncertain application")
	}
	saved, err := c.Store.Load()
	if err != nil || saved.LSN != baseline.LSN || !equalJSON(saved.Rows, baseline.Rows) {
		t.Fatal("rejected target advanced source checkpoint")
	}
	cancel, done = runCapture(t, c, f.dsn, func(context.Context, *State, *State) error { return nil })
	awaitCapture(t, c, func(s *State) bool { return string(s.Rows["1"]["title"]) == `"pending"` })
	stopCapture(t, cancel, done)
}
func TestCaptureRejectsRequestedProjectionBeforeCreatingSlot(t *testing.T) {
	f, c := captureFixture(t)
	c.Selection.Columns = []string{"id", "metadata"}
	if _, err := c.Initialize(context.Background(), f.dsn); err == nil {
		t.Fatal("publication mismatch admitted")
	}
	config, _ := c.Selection.config()
	var slots int
	if err := f.sql.QueryRow(context.Background(), `SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1`, config.Slot).Scan(&slots); err != nil || slots != 0 {
		t.Fatal("rejected projection created a source slot")
	}
}
func TestExistingSourceStoreNeverCreatesMissingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	if _, err := OpenExistingStore(path); err == nil {
		t.Fatal("missing source volume created")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("source state file unexpectedly exists")
	}
}

func TestCaptureResumeRejectsEmptyCheckpointBeforeConnecting(t *testing.T) {
	store := openTestStore(t, filepath.Join(t.TempDir(), "source.db"))
	capture := &Capture{Selection: CaptureSelection{ConnectionID: "connection", Region: "ap-northeast-2", Schema: "public", Table: "items", Publication: "items_pub"}, Store: store}
	if _, err := capture.Resume(context.Background(), "invalid source address"); err == nil || err.Error() != "source checkpoint missing; recovery required" {
		t.Fatal("resume attempted a new snapshot", err)
	}
}
func TestTruncatedSourceStoreIsNotInitialized(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenExistingStore(path); err == nil {
		t.Fatal("truncated source store was initialized")
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 0 {
		t.Fatal("failed resume modified truncated state")
	}
}

func TestCaptureDoesNotCheckpointAfterLocalAuthorityExpiresDuringApply(t *testing.T) {
	f, c := captureFixture(t)
	baseline, err := c.Initialize(context.Background(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	var expired atomic.Bool
	denied := errors.New("authority expired")
	c.Authorize = func() error {
		if expired.Load() {
			return denied
		}
		return nil
	}
	cancel, done := runCapture(t, c, f.dsn, func(context.Context, *State, *State) error { expired.Store(true); return nil })
	defer cancel()
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET title='accepted-before-expiry' WHERE id=1")
	select {
	case err = <-done:
		if !errors.Is(err, denied) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("capture did not stop at authority expiry")
	}
	saved, err := c.Store.Load()
	if err != nil || saved.LSN != baseline.LSN {
		t.Fatal("expired authority advanced checkpoint", err)
	}
}

func TestIdleCaptureAdvancesFeedbackWithoutLosingOpenTransaction(t *testing.T) {
	f, c := captureFixture(t)
	ctx := context.Background()
	baseline, err := c.Initialize(ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	// This transaction starts before the heartbeat but commits after restart.
	tx, err := f.sql.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "UPDATE "+f.c.Schema+".items SET title='late commit' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	other, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(ctx)
	execSQL(t, other, "CREATE TABLE "+f.c.Schema+".unpublished(value text)")
	execSQL(t, other, "INSERT INTO "+f.c.Schema+".unpublished SELECT repeat('wal',1000) FROM generate_series(1,100)")
	var changed atomic.Int32
	apply := func(_ context.Context, old, next *State) error {
		if !equalJSON(old.Rows, next.Rows) {
			changed.Add(1)
		}
		return nil
	}
	cancel, done := runCapture(t, c, f.dsn, apply)
	saved := awaitCapture(t, c, func(s *State) bool { return s.LSN > baseline.LSN })
	if !equalJSON(saved.Rows, baseline.Rows) || changed.Load() != 0 {
		t.Fatal("heartbeat applied uncommitted rows")
	}
	config, _ := c.Selection.config()
	deadline := time.Now().Add(5 * time.Second)
	advanced := false
	for time.Now().Before(deadline) {
		var confirmed string
		if err = other.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", config.Slot).Scan(&confirmed); err != nil {
			t.Fatal(err)
		}
		lsn, e := pglogrepl.ParseLSN(confirmed)
		if e != nil {
			t.Fatal(e)
		}
		if uint64(lsn) >= saved.LSN {
			advanced = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopCapture(t, cancel, done)
	if !advanced {
		t.Fatal("idle slot feedback did not advance")
	}
	if _, err = c.Resume(ctx, f.dsn); err != nil {
		t.Fatal("heartbeat checkpoint not resumable", err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	cancel, done = runCapture(t, c, f.dsn, apply)
	awaitCapture(t, c, func(s *State) bool { return string(s.Rows["1"]["title"]) == `"late commit"` })
	stopCapture(t, cancel, done)
	if changed.Load() != 1 {
		t.Fatal("open transaction was lost or applied twice", changed.Load())
	}
}
