//go:build unix

package postgrescdc

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestLegacyInventoryCLIRepairAndCleanupAfterSIGKILL(t *testing.T) {
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
	mock.mu.Lock()
	mock.docs["unrelated-collection"] = map[string]map[string]any{}
	mock.mu.Unlock()
	first := startManagedProcess(t, binary, f, control, "synthetic", caPath)
	one := control.wait(t, first, func(o Observation) bool { return o.Phase == "active" })
	first.stop(t)
	waitInactive(t, f)
	second := startManagedProcess(t, binary, f, control, "synthetic", caPath)
	two := control.wait(t, second, func(o Observation) bool { return o.Phase == "active" && o.Epoch > one.Epoch })
	second.stop(t)
	waitInactive(t, f)
	// Emulate the historical format: latest execution/checkpoint remain, but it
	// never journaled all earlier creates. External test evidence retains both.
	mutateInventoryState(t, f.path, func(s *Store) error {
		return s.db.Update(func(tx *bolt.Tx) error { return tx.DeleteBucket([]byte("lifecycle")) })
	})
	control.lifecycleCommand(t, 2, true)
	blocked := startManagedProcess(t, binary, f, control, "synthetic", caPath)
	control.wait(t, blocked, func(o Observation) bool { return o.Phase == "action_required" && o.DesiredRevision == 2 })
	blocked.stop(t)
	inspect := func() InventoryInspection {
		t.Helper()
		raw, err := exec.Command(binary, "-inspect-inventory", "-state", f.path).Output()
		if err != nil {
			t.Fatal("inventory CLI failed", err)
		}
		var r InventoryInspection
		if json.Unmarshal(raw, &r) != nil {
			t.Fatal("invalid CLI metadata")
		}
		return r
	}
	report := inspect()
	if !report.ReconciliationAllowed || report.InventoryComplete || len(report.KnownGenerations) != 1 || report.KnownGenerations[0] != two.Generation {
		t.Fatal("legacy CLI inferred unrecorded generations")
	}
	request := InventoryReconciliation{Format: 1, BindingID: report.BindingID, ExpectedDesiredRevision: report.DesiredRevision, StateDigest: report.StateDigest, OwnedGenerations: []string{one.Generation, two.Generation}, OwnershipConfirmed: true}
	manifest := filepath.Join(t.TempDir(), "reviewed-inventory.json")
	if err := os.WriteFile(manifest, manifestBytes(t, request), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"-inspect-inventory", "-reconcile-inventory", manifest, "-state", f.path},
		{"-inspect-inventory", "-control-url", control.server.URL, "-state", f.path},
		{"-inspect-inventory", "-search-listen", "127.0.0.1:0", "-state", f.path},
		{"-inspect-inventory", "-state", f.path, "unexpected"},
	} {
		if _, err := exec.Command(binary, args...).Output(); err == nil {
			t.Fatal("maintenance accepted a mixed capture/search invocation")
		}
	}
	before := inventoryStateBytes(t, f.path)
	for _, point := range []string{"inventory_reconcile_write", "inventory_reconcile_committed"} {
		p := &workerProcess{done: make(chan struct{}), boundary: filepath.Join(t.TempDir(), "boundary")}
		p.cmd = exec.Command(binary, "-reconcile-inventory", manifest, "-state", f.path)
		p.cmd.Env = append(os.Environ(), "CDC_TEST_STOP_AT="+point, "CDC_TEST_BOUNDARY_FILE="+p.boundary)
		p.cmd.Stderr = &p.stderr
		if err := p.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		go func() { p.err = p.cmd.Wait(); close(p.done) }()
		t.Cleanup(func() {
			select {
			case <-p.done:
			default:
				_ = p.cmd.Process.Kill()
				<-p.done
			}
		})
		p.killAt(t, point)
		after := inspect()
		if point == "inventory_reconcile_write" && (after.InventoryComplete || after.StateDigest != report.StateDigest) {
			t.Fatal("uncommitted repair survived SIGKILL")
		}
		if point == "inventory_reconcile_committed" && (!after.InventoryComplete || after.Reconciliation == nil || len(after.KnownGenerations) != 2) {
			t.Fatal("committed repair was lost")
		}
	}
	raw, err := exec.Command(binary, "-reconcile-inventory", manifest, "-state", f.path).Output()
	if err != nil || !bytes.Contains(raw, []byte(`"inventory_complete":true`)) {
		t.Fatal("CLI receipt retry failed", err)
	}
	for bucket, original := range before {
		if bucket != "lifecycle" && !bytes.Equal(original, inventoryStateBytes(t, f.path)[bucket]) {
			t.Fatal("offline repair changed recovery data", bucket)
		}
	}
	// No source or target resources change until the approved worker restarts.
	mock.mu.Lock()
	count := len(mock.docs)
	mock.mu.Unlock()
	if count != 3 {
		t.Fatal("repair contacted/deleted target resources")
	}
	resumed := startManagedProcess(t, binary, f, control, "synthetic", caPath)
	control.wait(t, resumed, func(o Observation) bool { return o.Phase == "removed" && o.DesiredRevision == 2 })
	resumed.stop(t)
	waitInactive(t, f)
	mock.mu.Lock()
	remaining := len(mock.docs)
	_, kept := mock.docs["unrelated-collection"]
	mock.mu.Unlock()
	if remaining != 1 || !kept {
		t.Fatal("reviewed cleanup lost unrelated resources or leaked old owned generations")
	}
	var slots int
	if err = f.sql.QueryRow(context.Background(), "SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", f.c.Slot).Scan(&slots); err != nil || slots != 0 {
		t.Fatal("owned source slot not removed", err)
	}
	t.Log("legacy ownership review, real CLI atomic SIGKILL recovery, receipt retry and central-authorized owned-only cleanup verified")
}
