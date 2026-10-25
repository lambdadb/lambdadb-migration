package postgrescdc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func legacyInventoryFixture(t *testing.T) (string, InventoryReconciliation) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "checkpoint.db")
	store := openTestStore(t, path)
	c := testConfig()
	_ = c.ValidateIdentity()
	state := decoderState()
	state.Fingerprint = c.fingerprint()
	state.Rows["1"]["body"] = json.RawMessage(`"private-source-value-748"`)
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	a := approvedTest(c)
	a.Control.DesiredRevision = 3
	a.Control.DesiredState = "paused"
	if err := store.AcceptControl(a, c.BindingID); err != nil {
		t.Fatal(err)
	}
	old := "cdc_test_" + strings.Repeat("a", 24)
	recent := "cdc_test_" + strings.Repeat("b", 24)
	e := &Execution{Format: 1, Owner: Generation{Epoch: 2, Name: recent, Fingerprint: c.fingerprint(), ExpiresAt: time.Now().Add(-time.Minute), ExpectedActiveEpoch: 1}, Active: &Selection{Epoch: 1, Generation: old, BarrierLSN: 100, Digest: "verified"}}
	if err := store.db.Update(func(tx *bolt.Tx) error { return saveExecution(tx, e) }); err != nil {
		t.Fatal(err)
	}
	if err := store.saveBootstrap(bootstrap{Fingerprint: c.fingerprint(), SystemID: "private-system", Timeline: 1, Database: "private-database"}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	report, err := InspectInventory(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, InventoryReconciliation{Format: 1, BindingID: c.BindingID, ExpectedDesiredRevision: 3, StateDigest: report.StateDigest, OwnedGenerations: []string{old, recent, "cdc_test_" + strings.Repeat("c", 24)}, OwnershipConfirmed: true}
}
func manifestBytes(t *testing.T, r InventoryReconciliation) []byte {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func inventoryStateBytes(t *testing.T, path string) map[string][]byte {
	t.Helper()
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state := map[string][]byte{}
	err = store.db.View(func(tx *bolt.Tx) error {
		for _, entry := range []struct{ bucket, key string }{{"managed_control", "approved"}, {"execution", "state"}, {"cdc", "state"}, {"bootstrap", "owner"}, {"lifecycle", "state"}} {
			if b := tx.Bucket([]byte(entry.bucket)); b != nil {
				state[entry.bucket] = bytes.Clone(b.Get([]byte(entry.key)))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func mutateInventoryState(t *testing.T, path string, edit func(*Store) error) {
	t.Helper()
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = edit(store); err != nil {
		t.Fatal(err)
	}
}
func TestInventoryInspectionIsReadOnlyAndValueSafe(t *testing.T) {
	path, _ := legacyInventoryFixture(t)
	before, _ := os.ReadFile(path)
	report, err := InspectInventory(path)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) || !report.ReconciliationAllowed || report.InventoryComplete || report.RowCount != 1 || report.Epoch != 2 || len(report.KnownGenerations) != 2 {
		t.Fatal("inspection changed state or lost metadata")
	}
	raw, _ := json.Marshal(report)
	for _, secret := range []string{"private-source-value-748", "private-system", "private-database", "TEST_DSN", "TEST_KEY", "https://test.invalid", "index_configs"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("inspection exposed %s", secret)
		}
	}
}
func TestInventoryRepairPreservesRecoveryStateAndReplaysReceipt(t *testing.T) {
	path, request := legacyInventoryFixture(t)
	before := inventoryStateBytes(t, path)
	first, err := ReconcileInventory(path, bytes.NewReader(manifestBytes(t, request)))
	if err != nil {
		t.Fatal(err)
	}
	after := inventoryStateBytes(t, path)
	for bucket, raw := range before {
		if !bytes.Equal(raw, after[bucket]) {
			t.Fatal("repair changed recovery state", bucket)
		}
	}
	if !first.InventoryComplete || first.Reconciliation == nil || first.Reconciliation.ReviewedStateDigest != request.StateDigest || first.Reconciliation.DesiredRevision != 3 || first.ReconciliationAllowed || len(first.KnownGenerations) != 3 {
		t.Fatal("reviewed inventory/receipt not recorded")
	}
	again, err := ReconcileInventory(path, bytes.NewReader(manifestBytes(t, request)))
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatal("exact receipt retry was not idempotent", err)
	}
	// Reporting sequence is not resource/recovery state and cannot stale a review.
	path, request = legacyInventoryFixture(t)
	mutateInventoryState(t, path, func(s *Store) error { _, err := s.observationSequence(); return err })
	if _, err = ReconcileInventory(path, bytes.NewReader(manifestBytes(t, request))); err != nil {
		t.Fatal("telemetry counter invalidated ownership review", err)
	}
}
func TestInventoryRepairRejectsStaleUnauthorizedAndIncompletePlans(t *testing.T) {
	for _, scenario := range []string{"binding", "revision", "digest", "unconfirmed", "omitted-name", "duplicate", "foreign-prefix", "invalid-suffix", "over-budget", "running", "expired", "unfenced", "changed-row", "changed-epoch", "changed-command", "collision", "already-complete"} {
		t.Run(scenario, func(t *testing.T) {
			path, request := legacyInventoryFixture(t)
			switch scenario {
			case "binding":
				request.BindingID = "other"
			case "revision":
				request.ExpectedDesiredRevision++
			case "digest":
				request.StateDigest = strings.Repeat("0", 64)
			case "unconfirmed":
				request.OwnershipConfirmed = false
			case "omitted-name":
				request.OwnedGenerations = request.OwnedGenerations[1:]
			case "duplicate":
				request.OwnedGenerations = append(request.OwnedGenerations, request.OwnedGenerations[0])
			case "foreign-prefix":
				request.OwnedGenerations = append(request.OwnedGenerations, "cdc_other_"+strings.Repeat("d", 24))
			case "invalid-suffix":
				request.OwnedGenerations = append(request.OwnedGenerations, "cdc_test_other")
			case "over-budget":
				for len(request.OwnedGenerations) <= maxTrackedGenerations {
					request.OwnedGenerations = append(request.OwnedGenerations, fmt.Sprintf("cdc_test_%024x", len(request.OwnedGenerations)))
				}
			case "running", "expired", "changed-command":
				mutateInventoryState(t, path, func(s *Store) error {
					a, err := s.Approved()
					if err != nil {
						return err
					}
					a.Control.DesiredRevision++
					if scenario == "running" {
						a.Control.DesiredState = "running"
					}
					if scenario == "expired" {
						a.Control.AuthorityExpiresAt = time.Now().Add(-time.Second)
					}
					return s.AcceptControl(*a, a.Binding.BindingID)
				})
			case "unfenced", "changed-epoch":
				mutateInventoryState(t, path, func(s *Store) error {
					return s.db.Update(func(tx *bolt.Tx) error {
						e, err := loadExecution(tx)
						if err != nil {
							return err
						}
						if scenario == "unfenced" {
							e.Owner.ExpiresAt = time.Now().Add(time.Hour)
						} else {
							e.Owner.Epoch++
						}
						return saveExecution(tx, e)
					})
				})
			case "changed-row":
				mutateInventoryState(t, path, func(s *Store) error {
					state, err := s.Load()
					if err != nil {
						return err
					}
					state.Rows["1"]["body"] = json.RawMessage(`"changed"`)
					return s.Save(state)
				})
			case "collision", "already-complete":
				mutateInventoryState(t, path, func(s *Store) error {
					a, _ := s.Approved()
					return s.db.Update(func(tx *bolt.Tx) error {
						return saveLifecycle(tx, &lifecycleState{Format: 1, Fingerprint: a.Binding.fingerprint(), Generations: map[string]bool{}, NameCollision: scenario == "collision", LegacyUnknown: scenario == "collision"})
					})
				})
			}
			before := inventoryStateBytes(t, path)
			if _, err := ReconcileInventory(path, bytes.NewReader(manifestBytes(t, request))); err == nil {
				t.Fatal("unsafe inventory review accepted")
			}
			if !reflect.DeepEqual(before, inventoryStateBytes(t, path)) {
				t.Fatal("rejected review changed regional state")
			}
		})
	}
}
func TestInventoryManifestParsingAndOfflineFileRequirements(t *testing.T) {
	path, request := legacyInventoryFixture(t)
	valid := manifestBytes(t, request)
	for _, raw := range [][]byte{nil, []byte(`{}`), append(bytes.Clone(valid), []byte(` {}`)...), append([]byte(`{"unknown":true,`), valid[1:]...), append(bytes.Repeat([]byte(" "), 16<<10), valid...), append(bytes.Clone(valid), bytes.Repeat([]byte(" "), 16<<10)...), []byte(strings.Replace(string(valid), `"owned_generations":[`, `"owned_generations":null,"invalid":[`, 1))} {
		if _, err := ReconcileInventory(path, bytes.NewReader(raw)); err == nil {
			t.Fatal("invalid manifest accepted")
		}
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	if _, err := InspectInventory(missing); err == nil {
		t.Fatal("missing state inspected")
	}
	if _, err := ReconcileInventory(missing, bytes.NewReader(valid)); err == nil {
		t.Fatal("missing state created")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("maintenance created missing state")
	}
	empty := filepath.Join(t.TempDir(), "empty.db")
	_ = os.WriteFile(empty, nil, 0600)
	if _, err := ReconcileInventory(empty, bytes.NewReader(valid)); err == nil {
		t.Fatal("empty state repaired")
	}
	raw, _ := os.ReadFile(empty)
	if len(raw) != 0 {
		t.Fatal("maintenance initialized an invalid state file")
	}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = InspectInventory(path); err == nil {
		t.Fatal("inspection bypassed worker lock")
	}
	if _, err = ReconcileInventory(path, bytes.NewReader(valid)); err == nil {
		t.Fatal("repair bypassed worker lock")
	}
}
func TestRemovedLegacyInventoryCanBeReconciledWithoutResurrection(t *testing.T) {
	path, _ := legacyInventoryFixture(t)
	mutateInventoryState(t, path, func(s *Store) error {
		a, _ := s.Approved()
		a.Control.ProtocolVersion = 2
		a.Control.DesiredRevision++
		a.Control.DesiredState = "removed"
		return s.AcceptControl(*a, a.Binding.BindingID)
	})
	report, err := InspectInventory(path)
	if err != nil || !report.ReconciliationAllowed {
		t.Fatal("terminal pending removal cannot be repaired", err)
	}
	request := InventoryReconciliation{Format: 1, BindingID: report.BindingID, ExpectedDesiredRevision: report.DesiredRevision, StateDigest: report.StateDigest, OwnedGenerations: report.KnownGenerations, OwnershipConfirmed: true}
	if _, err = ReconcileInventory(path, bytes.NewReader(manifestBytes(t, request))); err != nil {
		t.Fatal(err)
	}
	mutateInventoryState(t, path, func(s *Store) error {
		a, _ := s.Approved()
		if a.Control.DesiredState != "removed" {
			t.Fatal("inventory repair resumed terminal removal")
		}
		a.Control.DesiredRevision++
		a.Control.DesiredState = "running"
		if s.AcceptControl(*a, a.Binding.BindingID) == nil {
			t.Fatal("repaired terminal removal can resume")
		}
		return nil
	})
}

func TestInventoryInspectionRejectsMalformedMetadata(t *testing.T) {
	for _, scenario := range []string{"generation-name", "receipt", "source-fingerprint"} {
		t.Run(scenario, func(t *testing.T) {
			path, _ := legacyInventoryFixture(t)
			mutateInventoryState(t, path, func(s *Store) error {
				return s.db.Update(func(tx *bolt.Tx) error {
					a, _ := approved(tx)
					switch scenario {
					case "generation-name":
						e, _ := loadExecution(tx)
						e.Owner.Name = "private-source-value-748"
						return saveExecution(tx, e)
					case "receipt":
						return saveLifecycle(tx, &lifecycleState{Format: 1, Fingerprint: a.Binding.fingerprint(), Generations: map[string]bool{}, LegacyUnknown: true, Reconciliation: &InventoryReceipt{ManifestDigest: "private-source-value-748"}})
					default:
						b := tx.Bucket([]byte("bootstrap"))
						return b.Put([]byte("owner"), []byte(`{"Fingerprint":"foreign"}`))
					}
				})
			})
			if _, err := InspectInventory(path); err == nil {
				t.Fatal("malformed metadata exposed as valid inventory")
			}
		})
	}
}

func TestInventoryInspectionReportsUnreconcilableBudget(t *testing.T) {
	path, _ := legacyInventoryFixture(t)
	mutateInventoryState(t, path, func(s *Store) error {
		return s.db.Update(func(tx *bolt.Tx) error {
			a, _ := approved(tx)
			names := map[string]bool{}
			for i := 0; i < maxTrackedGenerations; i++ {
				names[fmt.Sprintf("cdc_test_%024x", i)] = true
			}
			return saveLifecycle(tx, &lifecycleState{Format: 1, Fingerprint: a.Binding.fingerprint(), Generations: names, LegacyUnknown: true})
		})
	})
	report, err := InspectInventory(path)
	if err != nil || report.ReconciliationAllowed || report.BlockedReason != "inventory_budget_exceeded" {
		t.Fatal("over-budget history advertised as repairable", err)
	}
}
