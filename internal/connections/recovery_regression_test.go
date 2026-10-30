package connections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
	"github.com/lambdadb/lambdadb-migration/internal/postgrescdc"
	"github.com/lambdadb/lambdadb-migration/internal/statefile"
	bolt "go.etcd.io/bbolt"
)

func TestUnsentAuthorityFailureRemainsPrepared(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
		if err != nil {
			t.Fatal(err)
		}
		calls, checks := 0, 0
		expired := errors.New("authority_expired")
		target := guardedTarget{intentSender(func(context.Context, Intent) error { calls++; return nil }), func() error {
			checks++
			if checks >= failAt {
				return expired
			}
			return nil
		}}
		if err = j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`)); !errors.Is(err, expired) {
			t.Fatal(err)
		}
		state, _ := j.Load()
		if calls != 0 || state.Pending.Status != "prepared" || j.PendingError() != nil {
			t.Fatal("unsent rejection poisoned journal")
		}
		if err = j.RecoverPending(context.Background(), intentSender(func(context.Context, Intent) error { calls++; return nil })); err != nil || calls != 1 {
			t.Fatal(err)
		}
		j.Close()
	}
}

func TestRateLimitRetainsIntentAndDelayAcrossRestart(t *testing.T) {
	var calls atomic.Int32
	target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(202)
		w.Write([]byte(`{"message":"accepted"}`))
	})
	path := filepath.Join(t.TempDir(), "state.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	var retry *retryTargetError
	err = j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`))
	if !errors.As(err, &retry) || time.Until(retry.at) < 29*time.Second {
		t.Fatal(err)
	}
	state, _ := j.Load()
	if state.Pending.Status != "prepared" || state.AcceptedSequence != 0 {
		t.Fatal("rate limit not safely retained")
	}
	j.Close()
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.RecoverPending(context.Background(), target); !errors.As(err, &retry) || calls.Load() != 1 {
		t.Fatal("retry ignored persisted server delay", err)
	}
	// Expire only the test clock boundary; production never clears an uncertain intent.
	err = j.db.Update(func(tx *bolt.Tx) error {
		s, e := j.decode(tx)
		if e != nil {
			return e
		}
		s.Pending.RetryAt = time.Now().Add(-time.Second)
		return j.write(tx, s)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = j.RecoverPending(context.Background(), target); err != nil || calls.Load() != 2 {
		t.Fatal(err)
	}
	state, _ = j.Load()
	if state.Pending != nil || state.AcceptedSequence != 1 {
		t.Fatal("retry not accepted")
	}
}

func TestAdmittedRequestDrainsOnCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(202)
		w.Write([]byte(`{"message":"accepted"}`))
	})
	j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- j.Apply(ctx, target, "delete", json.RawMessage(`{"ids":["one"]}`)) }()
	<-entered
	cancel()
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = j.PendingError(); err != nil {
		t.Fatal(err)
	}
}

func TestCancelledBeforeTransportNeverSends(t *testing.T) {
	var calls atomic.Int32
	target, _ := targetFixture(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var unsent unsentError
	err := target.Send(ctx, Intent{})
	if !errors.As(err, &unsent) || !errors.Is(err, context.Canceled) || calls.Load() != 0 {
		t.Fatal(err)
	}
}

func TestControlHistoryValidation(t *testing.T) {
	_, j, _ := inspectionFixture(t, "snapshot")
	raw, _ := j.Load()
	var d workerData
	json.Unmarshal(raw.Data, &d)
	previous := d.Approved
	previous.Control.DesiredRevision = 5
	previous.Control.ResyncRevision = 3
	for _, test := range []struct {
		name   string
		change func(*protocol.Approved)
		valid  bool
	}{
		{"identical", func(*protocol.Approved) {}, true},
		{"renewal", func(a *protocol.Approved) {
			a.Control.DesiredRevision++
			a.Control.AuthorityExpiresAt = a.Control.AuthorityExpiresAt.Add(time.Hour)
		}, true},
		{"same revision state", func(a *protocol.Approved) { a.Control.DesiredState = "paused" }, false},
		{"same revision expiry", func(a *protocol.Approved) { a.Control.AuthorityExpiresAt = a.Control.AuthorityExpiresAt.Add(time.Hour) }, false},
		{"desired regression", func(a *protocol.Approved) { a.Control.DesiredRevision-- }, false},
		{"resync regression", func(a *protocol.Approved) { a.Control.DesiredRevision++; a.Control.ResyncRevision-- }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := previous
			test.change(&next)
			if (validateControl(previous, next) == nil) != test.valid {
				t.Fatal("invalid command disposition")
			}
		})
	}
	next := previous
	previous.Control.DesiredState = "disconnected"
	next.Control.DesiredRevision++
	if validateControl(previous, next) == nil {
		t.Fatal("disconnected connection revived")
	}
}

func TestUnchangedLifecycleDoesNotCommit(t *testing.T) {
	_, j, source := inspectionFixture(t, "snapshot")
	w := &Worker{journal: j, source: source, fingerprint: j.fingerprint}
	var before, after int
	j.db.View(func(tx *bolt.Tx) error { before = tx.ID(); return nil })
	for i := 0; i < 10; i++ {
		if err := w.phase("running", ""); err != nil {
			t.Fatal(err)
		}
	}
	j.db.View(func(tx *bolt.Tx) error { after = tx.ID(); return nil })
	if after != before {
		t.Fatal("unchanged lifecycle committed", before, after)
	}
}

func TestCreationRateLimitIsRetryableWithoutAdoption(t *testing.T) {
	_, j, source := inspectionFixture(t, "snapshot")
	w := &Worker{journal: j, source: source, fingerprint: j.fingerprint}
	if err := w.update(func(d *workerData) error { d.CreationAttempt = false; d.TargetCreated = false; return nil }); err != nil {
		t.Fatal(err)
	}
	var creates atomic.Int32
	target, _ := targetFixture(t, func(out http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			if creates.Load() < 2 {
				out.WriteHeader(404)
				return
			}
			out.Write([]byte(`{"collection":{"projectName":"customer-project","collectionName":"items"}}`))
			return
		}
		if creates.Add(1) == 1 {
			out.Header().Set("Retry-After", "60")
			out.WriteHeader(429)
			return
		}
		out.WriteHeader(201)
	})
	var retry *retryTargetError
	if err := w.ensureTarget(context.Background(), target); !errors.As(err, &retry) {
		t.Fatal(err)
	}
	d, _ := w.data()
	if d.CreationAttempt || d.TargetCreated || time.Until(d.CreationRetryAt) < 59*time.Second {
		t.Fatal("creation rejection retained uncertain attempt")
	}
	if err := w.ensureTarget(context.Background(), target); !errors.As(err, &retry) || creates.Load() != 1 {
		t.Fatal("creation retried before server deadline", err)
	}
	if err := w.update(func(d *workerData) error { d.CreationRetryAt = time.Now().Add(-time.Second); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := w.ensureTarget(context.Background(), target); err != nil || creates.Load() != 2 {
		t.Fatal(err)
	}
	d, _ = w.data()
	if !d.TargetCreated {
		t.Fatal("creation not confirmed")
	}
}

func TestReportHeartbeatAvoidsRepeatedJournalCommits(t *testing.T) {
	_, j, source := inspectionFixture(t, "continuous")
	var reports atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reports.Add(1); w.WriteHeader(204) }))
	defer server.Close()
	w := &Worker{journal: j, source: source, fingerprint: j.fingerprint, Control: &Control{URL: server.URL, ID: "inspection-connection", Client: server.Client()}}
	if err := w.report(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, _ := w.data()
	for i := 0; i < 10; i++ {
		if err := w.report(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := w.data()
	if reports.Load() != 1 || after.Sequence != before.Sequence {
		t.Fatal("unchanged report fsynced or sent")
	}
	if err := w.phase("paused", ""); err != nil {
		t.Fatal(err)
	}
	if err := w.report(context.Background()); err != nil || reports.Load() != 2 {
		t.Fatal("phase transition not immediately reported", err)
	}
	w.lastReport = time.Now().Add(-5 * time.Second)
	if err := w.report(context.Background()); err != nil || reports.Load() != 3 {
		t.Fatal("heartbeat suppressed", err)
	}
}

func TestWorkerInitialResyncRevisionAndRestart(t *testing.T) {
	for _, mode := range []string{"snapshot", "continuous"} {
		t.Run(mode, func(t *testing.T) {
			f := newWorkerFixture(t, mode)
			f.approved.Control.DesiredRevision = 4
			f.approved.Control.ResyncRevision = 3
			cancel, done := f.worker(t, true)
			f.await(t, func() bool { return f.observation != nil && f.observation.Observation.CompletedResyncRevision == 3 })
			stopWorker(t, cancel, done)
			source, err := postgrescdc.OpenExistingStore(filepath.Join(f.dir, "source.db"))
			if err != nil {
				t.Fatal(err)
			}
			cut, err := source.CurrentCut(protocol.Fingerprint(f.approved.Binding))
			source.Close()
			if err != nil || cut == nil || cut.Revision != 3 {
				t.Fatal("initial revision lost", cut, err)
			}
			if mode == "continuous" {
				j, err := OpenJournal(filepath.Join(f.dir, "connection.db"), protocol.Fingerprint(f.approved.Binding), false)
				if err != nil {
					t.Fatal(err)
				}
				state, _ := j.Load()
				j.Close()
				var d workerData
				json.Unmarshal(state.Data, &d)
				if d.InitialRevision == nil || *d.InitialRevision != 3 {
					t.Fatal("initial snapshot was tagged with revision zero")
				}
			}
			f.mu.Lock()
			mutations := f.mutations
			previousSequence := f.observation.Observation.Sequence
			f.mu.Unlock()
			cancel, done = f.worker(t, false)
			f.await(t, func() bool {
				return f.phaseIs(map[string]string{"snapshot": "completed", "continuous": "running"}[mode]) && f.observation.Observation.Sequence > previousSequence
			})
			stopWorker(t, cancel, done)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.mutations != mutations {
				t.Fatal("restart repeated baseline")
			}
		})
	}
}

func TestWorkerRejectsConflictingHistoryAtStartupAndPoll(t *testing.T) {
	for _, startup := range []bool{true, false} {
		for _, change := range []string{"resync regression", "same revision", "disconnected revival"} {
			t.Run(fmt.Sprint(startup, "/", change), func(t *testing.T) {
				f := newWorkerFixture(t, "snapshot")
				f.approved.Control.DesiredRevision = 3
				f.approved.Control.ResyncRevision = 2
				if change == "disconnected revival" {
					f.approved.Control.DesiredState = "disconnected"
				}
				cancel, done := f.worker(t, true)
				f.await(t, func() bool { return f.phaseIs("completed") || f.phaseIs("disconnected") })
				if startup {
					stopWorker(t, cancel, done)
				}
				f.mu.Lock()
				switch change {
				case "resync regression":
					f.approved.Control.DesiredRevision++
					f.approved.Control.ResyncRevision = 1
				case "same revision":
					f.approved.Control.DesiredState = "paused"
				case "disconnected revival":
					f.approved.Control.DesiredRevision++
					f.approved.Control.DesiredState = "running"
				}
				f.mu.Unlock()
				if startup {
					cancel, done = f.worker(t, false)
				}
				defer cancel()
				select {
				case err := <-done:
					if err == nil || errors.Is(err, context.Canceled) {
						t.Fatal("invalid control was accepted", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("worker did not reject control")
				}
			})
		}
	}
}

func TestDirectorySyncFailurePreventsFirstObservation(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires directory permissions to be enforced")
	}
	f := newWorkerFixture(t, "snapshot")
	if err := os.Chmod(f.dir, 0300); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(f.dir, 0700)
	cancel, done := f.worker(t, true)
	defer cancel()
	select {
	case err := <-done:
		if !errors.Is(err, statefile.ErrUnavailable) {
			t.Fatal("directory sync failure was not fatal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("initialization did not fail")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.observation != nil || f.creates != 0 {
		t.Fatal("initialization advertised before durable directory entry")
	}
}

func TestCancellationAfterSendingMarkerButBeforeHTTPRemainsPrepared(t *testing.T) {
	var calls atomic.Int32
	api, _ := targetFixture(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	target := intentSender(func(ctx context.Context, p Intent) error { cancel(); return api.Send(ctx, p) })
	err = j.Apply(ctx, target, "delete", json.RawMessage(`{"ids":["one"]}`))
	state, _ := j.Load()
	if !errors.Is(err, context.Canceled) || state.Pending.Status != "prepared" || calls.Load() != 0 {
		t.Fatal("pre-transport cancellation became uncertain", err)
	}
}

func TestAdmittedRequestTimeoutRemainsUncertain(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); <-release; w.WriteHeader(202) })
	defer close(release)
	target.client.Timeout = 100 * time.Millisecond
	j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	err = j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`))
	if !errors.Is(err, ErrTargetOutcomeUncertain) {
		t.Fatal(err)
	}
	if err = j.RecoverPending(context.Background(), target); !errors.Is(err, ErrTargetOutcomeUncertain) || calls.Load() != 1 {
		t.Fatal("timed-out send retried", err)
	}
}

func TestRateLimitHTTPDateAndInvalidDelay(t *testing.T) {
	future := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	for _, test := range []struct {
		header  string
		minimum time.Duration
	}{
		{future.Format(http.TimeFormat), 59 * time.Minute}, {"invalid", 4 * time.Second}, {"-1", 4 * time.Second}, {"0", 4 * time.Second},
	} {
		var retry *retryTargetError
		if !errors.As(rateLimited(test.header), &retry) || time.Until(retry.at) < test.minimum {
			t.Fatal("invalid retry boundary", test.header)
		}
	}
}

func TestWorkerAutomaticallyRecoversRateLimitedSnapshot(t *testing.T) {
	f := newWorkerFixture(t, "snapshot")
	var limited atomic.Bool
	cancel, done := f.worker(t, true, func(w *Worker) {
		base := w.Transport
		w.Transport = partitionTransport(func(r *http.Request) (*http.Response, error) {
			if strings.HasSuffix(r.URL.Path, "/docs/upsert") && limited.CompareAndSwap(false, true) {
				return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"0"}}, Body: io.NopCloser(strings.NewReader(`{"message":"limited"}`)), Request: r}, nil
			}
			return base.RoundTrip(r)
		})
	})
	f.await(t, func() bool {
		return f.phaseIs("action_required") && f.observation.Observation.ReasonCode == "target_unavailable"
	})
	f.await(t, func() bool { return f.phaseIs("completed") && len(f.docs) == 2 })
	stopWorker(t, cancel, done)
}
