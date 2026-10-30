//go:build unix

package postgrescdc

import (
	"context"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lambdadb/go-lambdadb/models/apierrors"
)

func (m *processControl) lifecycleCommand(t *testing.T, rev uint64, remove bool) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.desired.Control.ProtocolVersion = 2
	m.desired.Control.DesiredRevision = rev
	m.desired.Control.AuthorityExpiresAt = time.Now().UTC().Add(15 * time.Minute)
	if remove {
		m.desired.Control.DesiredState = "removed"
	} else {
		m.desired.Control.DesiredState = "running"
		m.desired.Control.ResyncRevision = rev
	}
	if m.regionalURL != "" && m.publish() != 204 {
		t.Fatal("regional service rejected lifecycle protocol")
	}
}
func TestManagedLifecycleCrashRecovery(t *testing.T) { runManagedLifecycle(t, false) }
func TestLambdaDBLiveLifecycle(t *testing.T) {
	if os.Getenv("CDC_TEST_LIVE") != "1" {
		t.Skip("opt-in isolated LambdaDB target required")
	}
	runManagedLifecycle(t, true)
}
func runManagedLifecycle(t *testing.T, live bool) {
	f := newFixture(t)
	binary := buildProcessWorker(t)
	key := "synthetic-target-key"
	var mock *processTarget
	var target *LambdaTarget
	if live {
		f.c.TargetURL = os.Getenv("LAMBDADB_BASE_URL")
		f.c.TargetProject = os.Getenv("LAMBDADB_PROJECT_NAME")
		key = os.Getenv("LAMBDADB_PROJECT_API_KEY")
		if key == "" {
			t.Fatal("live target key required")
		}
		target = NewLambdaTarget(f.c, key)
	} else {
		mock = newProcessTarget(t)
		f.c.TargetURL = mock.server.URL
	}
	control := newProcessControl(t, f.c)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: control.server.Certificate().Raw})
	if mock != nil {
		ca = append(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: mock.server.Certificate().Raw})...)
	}
	caPath := filepath.Join(t.TempDir(), "lifecycle-ca.pem")
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	var children []*workerProcess
	unrelated := "unrelated-collection"
	if live {
		var err error
		unrelated, err = target.Create(context.Background(), f.c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			names := map[string]bool{unrelated: true}
			for _, child := range children {
				child.mu.Lock()
				for name := range child.seen {
					names[name] = true
				}
				child.mu.Unlock()
			}
			if store, err := OpenStore(f.path); err == nil {
				if l, err := store.lifecycle(); err == nil && l != nil {
					for name := range l.Generations {
						names[name] = true
					}
				}
				_ = store.Close()
			}
			for name := range names {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := target.Client.Collection(name).Delete(ctx)
				cancel()
				var missing *apierrors.ResourceNotFoundError
				if err != nil && !errors.As(err, &missing) {
					t.Errorf("test generation cleanup failed: %s", name)
				}
			}
		})
	} else {
		mock.mu.Lock()
		mock.docs[unrelated] = map[string]map[string]any{}
		mock.mu.Unlock()
	}
	startChild := func(point string) *workerProcess {
		p := startManagedProcessAt(t, binary, f, control, key, caPath, nil, point)
		children = append(children, p)
		return p
	}
	firstChild := startChild("")
	first := control.wait(t, firstChild, func(o Observation) bool { return o.Phase == "active" && o.DesiredRevision == 1 })
	firstChild.stop(t)
	waitInactive(t, f)
	// Lost continuity is repaired by an explicit new snapshot, not an unproven
	// continuation. Source mutations and the unrelated target remain untouched.
	execSQL(t, f.sql, "TRUNCATE "+f.c.Schema+".items; INSERT INTO "+f.c.Schema+".items VALUES(10,'resynced','new baseline','{}')")
	control.lifecycleCommand(t, 2, false)
	points := []string{"cleanup_slot_dropped", "cleanup_target_deleted", "cleanup_before_reset", "cleanup_reset_write", "cleanup_reset_committed"}
	if live {
		points = []string{"cleanup_target_deleted", "cleanup_reset_committed"}
	}
	for _, point := range points {
		child := startChild(point)
		child.killAt(t, point)
		waitInactive(t, f)
		// Once accepted, the command can resume from regional state without either
		// central management or regional HTTP delivery being available.
		control.unavailable(true)
	}
	recovered := startChild("")
	control.unavailable(false)
	second := control.wait(t, recovered, func(o Observation) bool {
		return o.Phase == "active" && o.DesiredRevision == 2 && o.CompletedResyncRevision == 2
	})
	if second.Epoch <= first.Epoch || second.Generation == first.Generation {
		t.Fatal("resync reused an old execution")
	}
	recovered.stop(t)
	waitInactive(t, f)
	state, _ := readProcessState(t, f)
	if !equalJSON(state.Rows, sourceRows(t, f)) {
		t.Fatal("new resync snapshot differs from full source state")
	}
	if live {
		assertLiveRows(t, target, second.Generation, state.Rows)
	} else {
		mock.assertRows(t, second.Generation, state.Rows)
		mock.mu.Lock()
		_, oldExists := mock.docs[first.Generation]
		mock.mu.Unlock()
		if oldExists {
			t.Fatal("old generation survived completed resync")
		}
	}
	// Remove while capture is running: the supervisor must await run exit before
	// dropping its slot or confirming generation cleanup.
	removing := startChild("")
	control.wait(t, removing, func(o Observation) bool { return o.Phase == "active" && o.Epoch > second.Epoch })
	control.lifecycleCommand(t, 3, true)
	control.wait(t, removing, func(o Observation) bool { return o.Phase == "removed" && o.DesiredRevision == 3 })
	removing.stop(t)
	waitInactive(t, f)
	store, err := OpenStore(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if state, err := store.Load(); err != nil || state != nil {
		t.Fatal("removed connection kept a recoverable source row mirror", err)
	}
	inventory, err := store.lifecycle()
	if err != nil || inventory == nil || !inventory.Removed || len(inventory.Generations) != 0 {
		t.Fatal("removed observation preceded durable cleanup", err)
	}
	var slots int
	if err = f.sql.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", f.c.Slot).Scan(&slots); err != nil || slots != 0 {
		t.Fatal("owned slot survived removal", err)
	}
	if !equalJSON(sourceRows(t, f), map[string]Row{"10": {"id": []byte("10"), "title": []byte(`"resynced"`), "body": []byte(`"new baseline"`), "metadata": []byte(`{}`)}}) {
		t.Fatal("cleanup changed customer source rows")
	}
	var pubs int
	if err = f.sql.QueryRow(context.Background(), "SELECT count(*) FROM pg_publication WHERE pubname=$1", f.c.Publication).Scan(&pubs); err != nil || pubs != 1 {
		t.Fatal("cleanup removed customer publication", err)
	}
	if live {
		if _, err = target.Client.Collection(unrelated).Get(context.Background()); err != nil {
			t.Fatal("cleanup removed an unrecorded test collection")
		}
	} else {
		mock.mu.Lock()
		remaining := len(mock.docs)
		_, kept := mock.docs[unrelated]
		mock.mu.Unlock()
		if remaining != 1 || !kept {
			t.Fatal("cleanup removed unrelated target resources")
		}
	}
	// Terminal removal must survive restart and cannot be turned into resume.
	a, _ := store.Approved()
	a.Control.DesiredRevision++
	a.Control.DesiredState = "running"
	if !errors.Is(store.AcceptControl(*a, f.c.BindingID), ErrConflictingControl) {
		t.Fatal("terminal removal was resumed")
	}
	t.Log("real SIGKILL lifecycle recovery, explicit resnapshot, owned-only cleanup and terminal removal verified")
}

func TestGenerationPlanCrashCanBeRemoved(t *testing.T) {
	f := newFixture(t)
	mock := newProcessTarget(t)
	f.c.TargetURL = mock.server.URL
	control := newProcessControl(t, f.c)
	binary := buildProcessWorker(t)
	ca := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: control.server.Certificate().Raw}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: mock.server.Certificate().Raw})...)
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	child := startManagedProcessAt(t, binary, f, control, "synthetic-key", caPath, nil, "generation_planned")
	child.killAt(t, "generation_planned")
	waitInactive(t, f)
	store, err := OpenStore(f.path)
	if err != nil {
		t.Fatal(err)
	}
	l, err := store.lifecycle()
	if err != nil || l == nil || len(l.Generations) != 1 {
		t.Fatal("generation intent was not durable before remote creation", err)
	}
	store.Close()
	mock.mu.Lock()
	created := len(mock.docs)
	mock.mu.Unlock()
	if created != 0 {
		t.Fatal("planned boundary performed remote creation")
	}
	control.lifecycleCommand(t, 2, true)
	recovered := startManagedProcess(t, binary, f, control, "synthetic-key", caPath)
	control.wait(t, recovered, func(o Observation) bool { return o.Phase == "removed" && o.DesiredRevision == 2 })
	recovered.stop(t)
}
