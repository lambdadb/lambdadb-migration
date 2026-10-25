package postgrescdc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lambdadb "github.com/lambdadb/go-lambdadb"
	"github.com/lambdadb/go-lambdadb/retry"
	bolt "go.etcd.io/bbolt"
)

func lifecycleApproval(c Config, remove bool) Approved {
	a := approvedTest(c)
	a.Control.ProtocolVersion = 2
	a.Control.DesiredRevision = 2
	if remove {
		a.Control.DesiredState = "removed"
	} else {
		a.Control.ResyncRevision = 2
	}
	return a
}
func TestLifecycleAuthorityInventoryAndEpochFences(t *testing.T) {
	c := testConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	if err := store.AcceptControl(approvedTest(c), c.BindingID); err != nil {
		t.Fatal(err)
	}
	run, err := store.begin(c, true)
	if err != nil {
		t.Fatal(err)
	}
	run, err = store.nameGeneration(run, "cdc_test_"+strings.Repeat("a", 24))
	if err != nil {
		t.Fatal(err)
	}
	a := lifecycleApproval(c, false)
	if err = store.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if store.checkManaged(run) == nil {
		t.Fatal("resync did not fence old execution")
	}
	if _, err = store.begin(c, true); err == nil {
		t.Fatal("capture began before cleanup")
	}
	if err = store.checkCleanup(a); err != nil {
		t.Fatal(err)
	}
	paused := a
	paused.Control.DesiredRevision++
	paused.Control.DesiredState = "paused"
	if err = store.AcceptControl(paused, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if store.checkCleanup(a) == nil {
		t.Fatal("pause did not cancel cleanup")
	}
	newer := a
	newer.Control.DesiredRevision = 4
	newer.Control.ResyncRevision = 4
	if err = store.AcceptControl(newer, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if store.checkCleanup(a) == nil {
		t.Fatal("superseded resync can clear current state")
	}
	expired := newer
	expired.Control.DesiredRevision++
	expired.Control.AuthorityExpiresAt = time.Now().Add(-time.Second)
	if err = store.AcceptControl(expired, c.BindingID); err != nil {
		t.Fatal(err)
	}
	if store.checkCleanup(expired) == nil {
		t.Fatal("expired cleanup authority accepted")
	}
	removed := lifecycleApproval(c, true)
	removed.Control.DesiredRevision = 6
	removed.Control.ResyncRevision = 4
	if err = store.AcceptControl(removed, c.BindingID); err != nil {
		t.Fatal(err)
	}
	newer.Control.DesiredRevision = 7
	if err = store.AcceptControl(newer, c.BindingID); !errors.Is(err, ErrConflictingControl) {
		t.Fatal("terminal removal resurrected", err)
	}
	inventory, _ := store.lifecycle()
	if len(inventory.Generations) != 1 || inventory.Removed {
		t.Fatal("invalid command altered ownership inventory")
	}
}
func TestLifecycleRefusesUnknownLegacyInventory(t *testing.T) {
	c := testConfig()
	_ = c.Validate()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	state := decoderState()
	state.Fingerprint = c.fingerprint()
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	a := lifecycleApproval(c, true)
	if err := store.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	// The refusal occurs before either source or target access.
	err := store.cleanup(context.Background(), a, "invalid source", nil)
	if err == nil || !strings.Contains(err.Error(), "legacy generation ownership") {
		t.Fatal("legacy ownership was guessed", err)
	}
	loaded, _ := store.Load()
	if !equalJSON(state, loaded) {
		t.Fatal("legacy refusal lost rows")
	}
}
func TestGenerationInventoryBudgetAndCollisionQuarantine(t *testing.T) {
	c := testConfig()
	_ = c.Validate()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	for i := 0; i < maxTrackedGenerations; i++ {
		run, err := store.beginGeneration(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.nameGeneration(run, fmt.Sprintf("cdc_test_%024x", i)); err != nil {
			t.Fatal(err)
		}
	}
	run, err := store.beginGeneration(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.nameGeneration(run, "cdc_test_"+strings.Repeat("f", 24)); err == nil {
		t.Fatal("unbounded generation inventory")
	}
	if err = store.quarantineInventory(); err != nil {
		t.Fatal(err)
	}
	l, err := store.lifecycle()
	if err != nil || !l.LegacyUnknown || len(l.Generations) != maxTrackedGenerations {
		t.Fatal("failed create/collision corrupted inventory", err)
	}
}

func lifecycleSnapshot(t *testing.T, f *fixture, store *Store) *State {
	t.Helper()
	ctx := context.Background()
	if err := f.c.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := store.AcceptControl(approvedTest(f.c), f.c.BindingID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.begin(f.c, true); err != nil {
		t.Fatal(err)
	}
	repl, err := replicationConn(ctx, f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repl.Close(ctx)
	state, err := snapshot(ctx, f.sql, repl, store, f.c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Save(state); err != nil {
		t.Fatal(err)
	}
	return state
}
func TestLifecycleRefusesUnownedSlotAndIncompatibleSchema(t *testing.T) {
	for _, scenario := range []string{"unowned", "changed-system", "incompatible-schema"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			store := openTestStore(t, f.path)
			if scenario == "unowned" {
				if _, err := f.sql.Exec(context.Background(), "SELECT pg_create_logical_replication_slot($1,'pgoutput')", f.c.Slot); err != nil {
					t.Fatal(err)
				}
			} else {
				state := lifecycleSnapshot(t, f, store)
				if scenario == "changed-system" {
					state.SystemID = "foreign-system"
					if err := store.Save(state); err != nil {
						t.Fatal(err)
					}
				} else {
					execSQL(t, f.sql, "ALTER TABLE "+f.c.Schema+".items ADD COLUMN incompatible text")
				}
			}
			a := lifecycleApproval(f.c, scenario != "incompatible-schema")
			if err := store.AcceptControl(a, f.c.BindingID); err != nil {
				t.Fatal(err)
			}
			if err := store.cleanup(context.Background(), a, f.dsn, nil); err == nil {
				t.Fatal("unproven cleanup succeeded")
			}
			var exists bool
			if err := f.sql.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", f.c.Slot).Scan(&exists); err != nil || !exists {
				t.Fatal("cleanup touched unowned/incompatible slot", err)
			}
		})
	}
}
func TestCleanupResponseLossRetainsStateUntilVerifiedRetry(t *testing.T) {
	f := newFixture(t)
	store := openTestStore(t, f.path)
	state := lifecycleSnapshot(t, f, store)
	run, err := store.Execution()
	if err != nil {
		t.Fatal(err)
	}
	name := "cdc_test_" + strings.Repeat("b", 24)
	if _, err = store.nameGeneration(run.Owner, name); err != nil {
		t.Fatal(err)
	}
	var deletes atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "DELETE" && deletes.Add(1) == 1 {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"message":"response lost after delete"}`))
			return
		}
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"message":"collection not found"}`))
	}))
	defer server.Close()
	target := &LambdaTarget{Client: lambdadb.New(lambdadb.WithBaseURL(server.URL), lambdadb.WithProjectName("test"), lambdadb.WithAPIKey("synthetic"), lambdadb.WithClient(server.Client()), lambdadb.WithRetryConfig(retry.Config{Strategy: "none"}))}
	a := lifecycleApproval(f.c, true)
	if err = store.AcceptControl(a, f.c.BindingID); err != nil {
		t.Fatal(err)
	}
	if err = store.cleanup(context.Background(), a, f.dsn, target); err == nil {
		t.Fatal("uncertain delete completed cleanup")
	}
	loaded, _ := store.Load()
	l, _ := store.lifecycle()
	if !equalJSON(loaded, state) || l.Removed || len(l.Generations) != 1 {
		t.Fatal("uncertain target outcome discarded recovery state")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.cleanup(context.Background(), a, f.dsn, target); err != nil {
		t.Fatal("idempotent 404 retry failed", err)
	}
	loaded, _ = store.Load()
	l, _ = store.lifecycle()
	if loaded != nil || !l.Removed || len(l.Generations) != 0 {
		t.Fatal("verified removal not durably recorded")
	}
	if err = store.db.View(func(tx *bolt.Tx) error {
		e, err := loadExecution(tx)
		if err == nil && (e.Owner.Epoch != run.Owner.Epoch || e.Active != nil || e.Owner.Name != "") {
			return errors.New("cleanup regressed epoch or retained selection")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
