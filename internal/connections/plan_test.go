package connections

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
	bolt "go.etcd.io/bbolt"
)

func transactionStates() (*postgrescdc.State, *postgrescdc.State) {
	old := &postgrescdc.State{Format: 1, Fingerprint: "capture", SystemID: "system", LSN: 100,
		Columns: []postgrescdc.Column{{Name: "pk", OID: 20, Key: true}, {Name: "title", OID: 25}},
		Rows:    map[string]postgrescdc.Row{"1": {"pk": json.RawMessage(`1`), "title": json.RawMessage(`"old"`)}}}
	next := *old
	next.LSN = 200
	next.Rows = map[string]postgrescdc.Row{"2": {"pk": json.RawMessage(`2`), "title": json.RawMessage(`"new"`)}}
	return old, &next
}

// The real subprocess exits only when killed, leaving a decoded commit and its
// accepted cursor durable but the old source checkpoint intact.
func TestApplicationPlanCrashHelper(t *testing.T) {
	dir := os.Getenv("CONNECTION_PLAN_CRASH_DIRECTORY")
	if dir == "" {
		t.Skip("test subprocess only")
	}
	j, err := OpenJournal(filepath.Join(dir, "connection.db"), "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	source, err := postgrescdc.OpenExistingStore(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	old, next := transactionStates()
	if err := source.PrepareTransaction(old, next); err != nil {
		t.Fatal(err)
	}
	w := &Worker{journal: j}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err = w.applyChanges(ctx, intentSender(func(context.Context, Intent) error {
		calls++
		if os.Getenv("CONNECTION_PLAN_CRASH_PARTIAL") == "1" {
			cancel()
		}
		return nil
	}), old, next)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	fmt.Printf("plan-durable:%d\n", calls)
	select {}
}

func TestApplicationPlanCrashDoesNotResendAcceptedOperations(t *testing.T) {
	for _, partial := range []bool{true, false} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			dir := t.TempDir()
			j, err := OpenJournal(filepath.Join(dir, "connection.db"), "binding", true)
			if err != nil {
				t.Fatal(err)
			}
			if err = j.Close(); err != nil {
				t.Fatal(err)
			}
			source, err := postgrescdc.OpenNewStore(filepath.Join(dir, "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			old, next := transactionStates()
			if err = source.Save(old); err != nil {
				t.Fatal(err)
			}
			source.Close()
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-test.run=^TestApplicationPlanCrashHelper$", "-test.timeout=20s")
			cmd.Env = append(os.Environ(), "CONNECTION_PLAN_CRASH_DIRECTORY="+dir)
			if partial {
				cmd.Env = append(cmd.Env, "CONNECTION_PLAN_CRASH_PARTIAL=1")
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			scanner := bufio.NewScanner(stdout)
			wantCalls := 2
			if partial {
				wantCalls = 1
			}
			ready := false
			for scanner.Scan() {
				if scanner.Text() == fmt.Sprintf("plan-durable:%d", wantCalls) {
					ready = true
					break
				}
			}
			if !ready {
				_ = cmd.Wait()
				t.Fatal("child did not reach the acceptance/source-checkpoint boundary")
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			j, err = OpenJournal(filepath.Join(dir, "connection.db"), "binding", false)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			source, err = postgrescdc.OpenExistingStore(filepath.Join(dir, "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			retained, err := source.PreparedTransaction()
			if err != nil || retained == nil || !reflect.DeepEqual(retained.Next, next) {
				t.Fatal("decoded transaction did not survive", err)
			}
			current, _ := source.Load()
			if !reflect.DeepEqual(current, old) {
				t.Fatal("source checkpoint advanced before acceptance")
			}
			var sent []string
			w := &Worker{journal: j}
			if err = w.applyChanges(t.Context(), intentSender(func(_ context.Context, p Intent) error {
				sent = append(sent, p.Operation)
				return nil
			}), current, retained.Next); err != nil {
				t.Fatal(err)
			}
			var want []string
			if partial {
				want = []string{"upsert"}
			}
			if !reflect.DeepEqual(sent, want) {
				t.Fatal("accepted operations were resent", sent)
			}
			if err = source.Save(retained.Next); err != nil {
				t.Fatal(err)
			}
			p, err := source.PreparedTransaction()
			if err != nil || p != nil {
				t.Fatal("source checkpoint did not clear the retained transaction", err)
			}
			s, err := j.Load()
			if err != nil || s.AcceptedSequence != 2 || s.Plan.Cursor != 2 || len(s.ManagedIDs) != 1 || !s.ManagedIDs["2"] {
				t.Fatal("acceptance/cursor/inventory disagreed", err)
			}
		})
	}
}

func TestApplicationPlanPendingRecoveryAdvancesExactCursor(t *testing.T) {
	j, err := OpenJournal(filepath.Join(t.TempDir(), "connection.db"), "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	id := fmt.Sprintf("%064x", 1)
	if _, err = j.beginPlan(id, 2); err != nil {
		t.Fatal(err)
	}
	if _, err = j.prepare("delete", json.RawMessage(`{"ids":["old"]}`), id, 1); err == nil {
		t.Fatal("skipped plan item")
	}
	p, err := j.prepare("delete", json.RawMessage(`{"ids":["old"]}`), id, 0)
	if err != nil {
		t.Fatal(err)
	}
	wrong := *p
	wrong.PlanIndex++
	if err = j.markSending(wrong); err == nil {
		t.Fatal("changed plan cursor accepted")
	}
	if err = j.RecoverPending(t.Context(), intentSender(func(context.Context, Intent) error { return nil })); err != nil {
		t.Fatal(err)
	}
	s, err := j.Load()
	if err != nil || s.Plan.Cursor != 1 || s.AcceptedSequence != 1 || s.Pending != nil {
		t.Fatal("prepared recovery did not atomically advance plan", err)
	}
	if _, err = j.beginPlan(fmt.Sprintf("%064x", 2), 1); err == nil {
		t.Fatal("incomplete plan replaced")
	}
	p, err = j.prepare("delete", json.RawMessage(`{"ids":["other"]}`), id, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.markSending(*p); err != nil {
		t.Fatal(err)
	}
	if err = j.RecoverPending(t.Context(), intentSender(func(context.Context, Intent) error {
		t.Fatal("uncertain plan item was resent")
		return nil
	})); !errors.Is(err, ErrTargetOutcomeUncertain) {
		t.Fatal(err)
	}
	s, err = j.Load()
	if err != nil || s.Plan.Cursor != 1 {
		t.Fatal("uncertain item advanced cursor", err)
	}
}

func TestOrdinaryJournalFormatThreeUpgradesWithoutLosingPendingEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connection.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	p, err := j.Prepare("delete", json.RawMessage(`{"ids":["old"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = j.markSending(*p); err != nil {
		t.Fatal(err)
	}
	if err = j.db.Update(func(tx *bolt.Tx) error {
		s, err := j.decode(tx)
		if err != nil {
			return err
		}
		s.Format = 3
		return j.write(tx, s)
	}); err != nil {
		t.Fatal(err)
	}
	j.Close()
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if !errors.Is(j.PendingError(), ErrTargetOutcomeUncertain) {
		t.Fatal("old journal sending evidence was lost")
	}
	if err = j.SaveData(json.RawMessage(`{"upgraded":true}`)); err != nil {
		t.Fatal(err)
	}
	if err = j.db.View(func(tx *bolt.Tx) error {
		var s State
		if json.Unmarshal(tx.Bucket([]byte("connection")).Get([]byte("state")), &s) != nil || s.Format != 4 || s.Pending == nil || s.Pending.Status != "sending" {
			t.Fatal("upgrade did not preserve pending intent")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
