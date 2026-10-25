//go:build unix

package postgrescdc

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestLambdaDBLiveProcessCrash(t *testing.T) {
	if os.Getenv("CDC_TEST_LIVE") != "1" {
		t.Skip("opt-in live target credentials required")
	}
	f := newFixture(t)
	f.c.TargetURL = os.Getenv("LAMBDADB_BASE_URL")
	f.c.TargetProject = os.Getenv("LAMBDADB_PROJECT_NAME")
	key := os.Getenv("LAMBDADB_PROJECT_API_KEY")
	if key == "" {
		t.Fatal("live target key required")
	}
	target := NewLambdaTarget(f.c, key)
	binary := buildProcessWorker(t)
	var processes []*workerProcess
	t.Cleanup(func() {
		names := map[string]bool{}
		for _, p := range processes {
			p.mu.Lock()
			for name := range p.seen {
				names[name] = true
			}
			p.mu.Unlock()
		}
		// A boundary can stop after naming a collection but before reporting it.
		if store, err := OpenStore(f.path); err == nil {
			if e, err := store.Execution(); err == nil && e.Owner.Name != "" {
				names[e.Owner.Name] = true
			}
			_ = store.Close()
		}
		for name := range names {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := target.Client.Collection(name).Delete(ctx)
			cancel()
			if err != nil {
				t.Errorf("live process generation cleanup failed: %s", name)
			}
		}
	})
	startChild := func(point string) *workerProcess {
		p := startProcess(t, binary, f, "", key, point)
		processes = append(processes, p)
		return p
	}
	seed := startChild("")
	first := seed.phase(t, "active")
	seed.stop(t)
	waitInactive(t, f)
	baseline, initial := readProcessState(t, f)
	if initial.Active == nil || initial.Active.Epoch != first.Epoch {
		t.Fatal("live seed was not verified and selected")
	}
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=9,title='moved',metadata='{}' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(2,'reinsert','newer','null')")
	for _, point := range []string{"after_apply", "after_save"} {
		child := startChild(point)
		child.killAt(t, point)
		waitInactive(t, f)
		state, e := readProcessState(t, f)
		if e.Active == nil || *e.Active != *initial.Active {
			t.Fatal("unverified live generation was selected")
		}
		if point == "after_apply" && (state.LSN < baseline.LSN || !equalJSON(state.Rows, baseline.Rows)) {
			t.Fatal("live accepted write advanced checkpoint before save")
		}
		if point == "after_save" && (state.LSN <= baseline.LSN || state.Rows["9"] == nil) {
			t.Fatal("live saved row was not durable")
		}
		t.Logf("real LambdaDB SDK process killed with SIGKILL at %s", point)
	}
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=10,title='final',metadata='{}' WHERE id=9; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(2,'final reinsert','latest','null')")
	recovered := startChild("")
	progress := recovered.phase(t, "active")
	recovered.stop(t)
	waitInactive(t, f)
	state, e := readProcessState(t, f)
	if !equalJSON(state.Rows, sourceRows(t, f)) || e.Active == nil || e.Active.Epoch != progress.Epoch || e.Active.Epoch <= initial.Active.Epoch {
		t.Fatal("live process recovery/source selection mismatch")
	}
	assertLiveRows(t, target, progress.Generation, state.Rows)
	if err := target.Delete(context.Background(), first.Generation, "2"); err != nil {
		t.Fatal(err)
	}
	assertLiveRows(t, target, progress.Generation, state.Rows)
	t.Logf("live SIGKILL recovery and verified conditional activation passed at epoch %d", progress.Epoch)
}
