package postgrescdc

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lambdadb "github.com/lambdadb/go-lambdadb"
)

// responseLoss exercises the real SDK write path, then discards a successful
// response. The worker must not checkpoint or issue a later mutation.
type responseLoss struct {
	Target
	lose atomic.Bool
}

type trackedTarget struct {
	Target
	mu          sync.Mutex
	generations []string
}

func (t *trackedTarget) Create(ctx context.Context, c Config) (string, error) {
	gen, err := t.Target.Create(ctx, c)
	if err == nil {
		t.mu.Lock()
		t.generations = append(t.generations, gen)
		t.mu.Unlock()
	}
	return gen, err
}

func (t *responseLoss) Put(ctx context.Context, gen, id string, row Row) error {
	if err := t.Target.Put(ctx, gen, id, row); err != nil {
		return err
	}
	if t.lose.Swap(false) {
		return errors.New("injected target response loss")
	}
	return nil
}

func TestLambdaDBLive(t *testing.T) {
	if os.Getenv("CDC_TEST_LIVE") != "1" {
		t.Skip("set CDC_TEST_LIVE=1 and isolated LambdaDB target credentials")
	}
	f := newFixture(t)
	f.c.TargetURL = os.Getenv("LAMBDADB_BASE_URL")
	f.c.TargetProject = os.Getenv("LAMBDADB_PROJECT_NAME")
	key := os.Getenv("LAMBDADB_PROJECT_API_KEY")
	if key == "" {
		t.Fatal("LAMBDADB_PROJECT_API_KEY required")
	}
	if err := f.c.Validate(); err != nil {
		t.Fatal(err)
	}
	target := NewLambdaTarget(f.c, key)
	store := openTestStore(t, f.path)
	// Track only generations created by this test; never clear an existing target.
	tracked := &trackedTarget{Target: target}
	t.Cleanup(func() {
		tracked.mu.Lock()
		defer tracked.mu.Unlock()
		for _, gen := range tracked.generations {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := target.Client.Collection(gen).Delete(ctx)
			cancel()
			if err != nil {
				t.Errorf("cleanup of test generation %s failed", gen)
			}
		}
	})
	// A source transaction commits while the exported snapshot is held.
	w := &Worker{Config: f.c, Store: store, Target: tracked, afterSnapshotImport: func() error {
		_, err := f.sql.Exec(context.Background(), "UPDATE "+f.c.Schema+".items SET title='boundary',metadata='{\"a\":3}' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(3,'insert','body','{}')")
		return err
	}}
	r := start(t, w, f.dsn)
	p := waitStreaming(t, r)
	s := waitRows(t, r, store, func(s *State) bool { return s.Rows["3"] != nil })
	assertLiveRows(t, target, p.Generation, s.Rows)
	if !equalJSON(s.Rows, sourceRows(t, f)) {
		t.Fatal("live initial catch-up differs from source")
	}
	stop(t, r)
	t.Logf("snapshot handoff verified; generation=%s", p.Generation)
	// Rebuild the saved baseline in a second generation, then lose a real write
	// response during a PK change with unchanged out-of-line text.
	loss := &responseLoss{Target: tracked}
	r = start(t, &Worker{Config: f.c, Store: store, Target: loss}, f.dsn)
	p2 := waitStreaming(t, r)
	// Test seam runs after rebuilding, before the next source transaction.
	loss.lose.Store(true)
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=10,title='moved',metadata='{}' WHERE id=1")
	select {
	case err := <-r.done:
		if err == nil || err.Error() != "injected target response loss" {
			t.Fatalf("unexpected response loss result: %v", err)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("response loss timeout")
	}
	r.cancel()
	s, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if s.Rows["1"] == nil || s.Rows["10"] != nil {
		t.Fatal("response loss advanced durable checkpoint")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, f.path)
	// More source changes while no worker is running.
	execSQL(t, f.sql, "DELETE FROM "+f.c.Schema+".items WHERE id=3; INSERT INTO "+f.c.Schema+".items VALUES(3,'reinsert','newer','null')")
	r = start(t, &Worker{Config: f.c, Store: store, Target: tracked}, f.dsn)
	p3 := waitStreaming(t, r)
	s = waitRows(t, r, store, func(s *State) bool { return s.Rows["10"] != nil && string(s.Rows["3"]["title"]) == `"reinsert"` })
	if !equalJSON(s.Rows, sourceRows(t, f)) {
		t.Fatal("live restart differs from source")
	}
	assertLiveRows(t, target, p3.Generation, s.Rows)
	// An old generation can still receive a delayed write. It cannot address the
	// replacement generation, even with the same source/document identifiers.
	if err = target.Delete(context.Background(), p2.Generation, "10"); err != nil {
		t.Fatal(err)
	}
	assertLiveRows(t, target, p3.Generation, s.Rows)
	stop(t, r)
	t.Logf("restart, response loss, PK/TOAST, JSON replacement and stale generation isolation verified; generation=%s", p3.Generation)
}

func assertLiveRows(t *testing.T, target *LambdaTarget, generation string, expected map[string]Row) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	// List all documents, not counts or selected IDs: detect stale/deleted keys.
	want := map[string]any{}
	for id, row := range expected {
		want[id] = document(id, row)
	}
	for {
		docs, err := target.Client.Collection(generation).Docs().ListAll(ctx, &lambdadb.ListDocsOpts{IncludeVectors: lambdadb.Bool(true)})
		if err != nil {
			t.Fatal("live target list failed")
		}
		got := map[string]any{}
		for _, doc := range docs {
			id, ok := doc["id"].(string)
			if !ok {
				t.Fatal("target id missing")
			}
			got[id] = doc
		}
		if equalJSON(want, got) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("complete committed target PK/field comparison failed")
		case <-time.After(500 * time.Millisecond):
		}
	}
}
