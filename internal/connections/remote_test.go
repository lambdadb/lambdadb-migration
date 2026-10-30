package connections

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
	"github.com/lambdadb/lambdadb-migration/internal/statefile"
	"github.com/lambdadb/lambdadb-migration/internal/testutil/stateobjects"
)

func remoteState(t *testing.T) (*stateobjects.Store, *RemoteState) {
	t.Helper()
	objects := stateobjects.New()
	marker, err := statefile.InitializeStore(t.Context(), objects, "test/", "ap-northeast-2")
	if err != nil {
		t.Fatal(err)
	}
	return objects, &RemoteState{Prefix: "test/", StoreID: marker.ID, Epoch: 1, Objects: objects}
}

func TestRemoteWorkerColdRestartKeepsCollectionBaselineAndAcceptedOperations(t *testing.T) {
	for _, mode := range []string{"snapshot", "continuous"} {
		t.Run(mode, func(t *testing.T) {
			f := newWorkerFixture(t, mode)
			_, remote := remoteState(t)
			configure := func(w *Worker) { copy := *remote; w.RemoteState = &copy }
			cancel, done := f.worker(t, true, configure)
			wantPhase := "completed"
			if mode == "continuous" {
				wantPhase = "running"
			}
			f.await(t, func() bool { return f.phaseIs(wantPhase) && len(f.docs) == 2 })
			stopWorker(t, cancel, done)
			f.mu.Lock()
			mutations := f.mutations
			f.mu.Unlock()
			// The restart receives a completely new filesystem, including no cache lock.
			f.dir = t.TempDir()
			remote.Epoch++
			cancel, done = f.worker(t, false, configure)
			defer func() { stopWorker(t, cancel, done) }()
			f.await(t, func() bool { return f.phaseIs(wantPhase) })
			f.mu.Lock()
			if f.creates != 1 || f.mutations != mutations {
				f.mu.Unlock()
				t.Fatal("cold restart replayed the baseline or changed collection")
			}
			f.mu.Unlock()
			if mode == "continuous" {
				f.sql(t, "UPDATE TABLE SET title='after restart' WHERE pk=1; DELETE FROM TABLE WHERE pk=9007199254740993; INSERT INTO TABLE VALUES(3,'three','{}')")
				f.await(t, func() bool {
					return len(f.docs) == 2 && f.docs["3"] != nil && string(f.docs["1"]["title"]) == `"after restart"`
				})
				f.mu.Lock()
				want := map[string]map[string]json.RawMessage{
					"1": {"id": json.RawMessage(`"1"`), "pk": json.RawMessage(`1`), "title": json.RawMessage(`"after restart"`)},
					"3": {"id": json.RawMessage(`"3"`), "pk": json.RawMessage(`3`), "title": json.RawMessage(`"three"`)},
				}
				if !reflect.DeepEqual(f.docs, want) {
					f.mu.Unlock()
					t.Fatal("complete target rows differ after cold restart")
				}
				f.mu.Unlock()
			}
		})
	}
}

func TestRemoteSendingRecoveryCannotResendAfterLocalDiskLoss(t *testing.T) {
	objects, remote := remoteState(t)
	path := filepath.Join(t.TempDir(), "connection.db")
	j, err := OpenJournal(path, strings.Repeat("a", 64), true)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := statefile.Export(j.db)
	if err != nil {
		t.Fatal(err)
	}
	r, err := statefile.OpenReplica(t.Context(), objects, remote.Prefix, statefile.Binding{StoreID: remote.StoreID, Region: "ap-northeast-2", ConnectionID: "connection", Fingerprint: j.fingerprint}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Initialize(map[string]statefile.Image{"connection": journal, "source": {}}); err != nil {
		t.Fatal(err)
	}
	if err = j.db.Attach(r, "connection"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = j.Apply(t.Context(), intentSender(func(context.Context, Intent) error {
		calls++
		objects.SetFault(func(key string, after bool) error {
			if strings.HasSuffix(key, "head.json") && !after {
				return errors.New("publication unavailable")
			}
			return nil
		})
		return nil
	}), "delete", json.RawMessage(`{"ids":["old"]}`))
	if !errors.Is(err, ErrTargetOutcomeUncertain) || calls != 1 {
		t.Fatal(err)
	}
	j.Close()
	objects.SetFault(nil)
	fresh, err := statefile.OpenReplica(t.Context(), objects, remote.Prefix, statefile.Binding{StoreID: remote.StoreID, Region: "ap-northeast-2", ConnectionID: "connection", Fingerprint: strings.Repeat("a", 64)}, 2)
	if err != nil {
		t.Fatal(err)
	}
	files, err := fresh.Recover()
	if err != nil {
		t.Fatal(err)
	}
	dir, err := statefile.Materialize(t.TempDir(), files)
	if err != nil {
		t.Fatal(err)
	}
	j, err = OpenJournal(filepath.Join(dir, "connection.db"), strings.Repeat("a", 64), false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.RecoverPending(t.Context(), intentSender(func(context.Context, Intent) error { calls++; return nil })); !errors.Is(err, ErrTargetOutcomeUncertain) || calls != 1 {
		t.Fatal("S3 restore erased uncertain acceptance", err)
	}
}

func TestRemoteSourceAndJournalRestoreAsOneSequence(t *testing.T) {
	objects, remote := remoteState(t)
	dir := t.TempDir()
	j, err := OpenJournal(filepath.Join(dir, "connection.db"), strings.Repeat("a", 64), true)
	if err != nil {
		t.Fatal(err)
	}
	s, err := postgrescdc.OpenNewStore(filepath.Join(dir, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	old, next := transactionStates()
	if err = s.Save(old); err != nil {
		t.Fatal(err)
	}
	ji, _ := statefile.Export(j.db)
	si, _ := s.Export()
	binding := statefile.Binding{StoreID: remote.StoreID, Region: "ap-northeast-2", ConnectionID: "connection", Fingerprint: j.fingerprint}
	r, err := statefile.OpenReplica(t.Context(), objects, remote.Prefix, binding, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Initialize(map[string]statefile.Image{"connection": ji, "source": si}); err != nil {
		t.Fatal(err)
	}
	if err = j.db.Attach(r, "connection"); err != nil {
		t.Fatal(err)
	}
	if err = s.AttachReplica(r); err != nil {
		t.Fatal(err)
	}
	if err = s.PrepareTransaction(old, next); err != nil {
		t.Fatal(err)
	}
	if err = (&Worker{journal: j}).applyChanges(t.Context(), intentSender(func(context.Context, Intent) error { return nil }), old, next); err != nil {
		t.Fatal(err)
	}
	j.Close()
	s.Close()
	if err = os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	fresh, _ := statefile.OpenReplica(t.Context(), objects, remote.Prefix, binding, 2)
	files, err := fresh.Recover()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := statefile.Materialize(t.TempDir(), files)
	if err != nil {
		t.Fatal(err)
	}
	j, err = OpenJournal(filepath.Join(restored, "connection.db"), binding.Fingerprint, false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	s, err = postgrescdc.OpenExistingStore(filepath.Join(restored, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = j.db.Attach(fresh, "connection"); err != nil {
		t.Fatal(err)
	}
	if err = s.AttachReplica(fresh); err != nil {
		t.Fatal(err)
	}
	p, err := s.PreparedTransaction()
	if err != nil || p == nil || p.FromLSN != 100 {
		t.Fatal(err)
	}
	if err := (&Worker{journal: j}).applyChanges(t.Context(), intentSender(func(context.Context, Intent) error {
		t.Fatal("accepted operation replayed after complete disk loss")
		return nil
	}), old, p.Next); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(p.Next); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteInspectionDoesNotClaimOrRevealPayloads(t *testing.T) {
	_, j, source := inspectionFixture(t, "continuous")
	objects, remote := remoteState(t)
	ji, err := statefile.Export(j.db)
	if err != nil {
		t.Fatal(err)
	}
	si, err := source.Export()
	if err != nil {
		t.Fatal(err)
	}
	binding := statefile.Binding{StoreID: remote.StoreID, Region: "ap-northeast-2", ConnectionID: "inspection-connection", Fingerprint: j.fingerprint}
	r, err := statefile.OpenReplica(t.Context(), objects, remote.Prefix, binding, 5)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Initialize(map[string]statefile.Image{"connection": ji, "source": si}); err != nil {
		t.Fatal(err)
	}
	before, _ := objects.Counters()
	out, err := InspectRemoteState(t.Context(), objects, remote.Prefix, binding)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := objects.Counters()
	raw, _ := json.Marshal(out)
	if before != after || !out.RemoteStateOnly || out.LocalStateOnly || out.ResumeAuthorized || strings.Contains(string(raw), "private-source-value") || strings.Contains(string(raw), "PRIVATE_SOURCE_REFERENCE") {
		t.Fatal("remote inspection mutated ownership or exposed payloads")
	}
}
