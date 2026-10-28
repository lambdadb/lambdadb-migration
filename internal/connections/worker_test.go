package connections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
	"github.com/lambdadb/lambdadb-migration/internal/statefile"
)

// workerFixture is an HTTP contract fixture. Engine/WAL integration is a separate
// optional cross-repository test; this fixture does not certify engine durability.
type workerFixture struct {
	mu                 sync.Mutex
	approved           protocol.Approved
	observation        *protocol.Observed
	server             *httptest.Server
	db                 *pgx.Conn
	credentials        string
	dir                string
	created            bool
	creates, mutations int
	docs               map[string]map[string]json.RawMessage
	secretsUnavailable bool
	controlUnavailable bool
	loseCreate         bool
	loseNext           bool
	afterMutation      func()
}
type fixtureSecrets struct{ f *workerFixture }

func (s fixtureSecrets) Resolve(_ context.Context, _, ref string) (string, error) {
	s.f.mu.Lock()
	defer s.f.mu.Unlock()
	if s.f.secretsUnavailable {
		return "", errors.New("fixture credentials retired")
	}
	if ref == "env:SOURCE" {
		return s.f.credentials, nil
	}
	return "local-target-key", nil
}
func newWorkerFixture(t *testing.T, mode string) *workerFixture {
	t.Helper()
	dsn := os.Getenv("CDC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("isolated local PostgreSQL required")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1") {
		t.Fatal("worker fixture requires localhost PostgreSQL")
	}
	db, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("cw_%d", time.Now().UnixNano())
	for _, sql := range []string{"CREATE SCHEMA " + schema, "CREATE TABLE " + schema + ".items(pk bigint PRIMARY KEY,title text,excluded jsonb)", "INSERT INTO " + schema + ".items VALUES(1,'one','{\"secret\":true}'),(9007199254740993,'large','{}')", "CREATE PUBLICATION " + schema + " FOR TABLE " + schema + ".items(pk,title)"} {
		if _, err = db.Exec(context.Background(), sql); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), "DROP PUBLICATION "+schema)
		_, _ = db.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = db.Close(context.Background())
	})
	conf, _ := pgx.ParseConfig(dsn)
	raw, _ := json.Marshal(PostgresCredentials{Host: conf.Host, Port: int(conf.Port), Database: conf.Database, Username: conf.User, Password: conf.Password, SSLMode: "disable"})
	f := &workerFixture{db: db, dir: t.TempDir(), credentials: string(raw), docs: map[string]map[string]json.RawMessage{}}
	f.server = httptest.NewTLSServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	publication := schema
	if mode == "snapshot" {
		publication = ""
	}
	selection, _ := json.Marshal(protocol.PostgresSelection{Schema: schema, Table: "items", Publication: publication, Columns: []string{"pk", "title"}})
	f.approved = protocol.Approved{Binding: protocol.Binding{ConnectionID: schema, Revision: 1, Region: "ap-northeast-2", Mode: mode, Source: protocol.Source{Type: "postgresql", CredentialRef: "env:SOURCE", Configuration: selection}, Target: protocol.Target{Version: 3, URL: f.server.URL, Project: "test", Collection: "items", CredentialRef: "env:TARGET"}, IndexConfigs: json.RawMessage(`{"title":{"type":"text"}}`)}, Control: protocol.Command{ProtocolVersion: 1, ConnectionID: schema, DesiredRevision: 1, DesiredState: "running", AuthorityExpiresAt: time.Now().Add(time.Hour)}}
	t.Cleanup(func() {
		hash := sha256.Sum256([]byte(schema))
		slot := "ldb_" + hex.EncodeToString(hash[:16])
		_, _ = db.Exec(context.Background(), `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1 AND NOT active`, slot)
	})
	return f
}
func (f *workerFixture) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	path := r.URL.Path
	if strings.HasPrefix(path, "/v1/connections/") {
		if f.controlUnavailable {
			w.WriteHeader(503)
			return
		}
		if strings.HasSuffix(path, "/desired") {
			_ = json.NewEncoder(w).Encode(f.approved)
			return
		}
		if r.Method == "GET" {
			if f.observation == nil {
				w.WriteHeader(404)
			} else {
				_ = json.NewEncoder(w).Encode(f.observation)
			}
			return
		}
		var o protocol.Observation
		if protocol.Decode(r.Body, &o) != nil || !o.Validate(f.approved.Binding.ConnectionID) || (f.observation != nil && o.Sequence <= f.observation.Observation.Sequence) {
			w.WriteHeader(409)
			return
		}
		f.observation = &protocol.Observed{Observation: o, ReceivedAt: time.Now()}
		w.WriteHeader(204)
		return
	}
	if r.Header.Get("x-api-key") != "local-target-key" {
		w.WriteHeader(401)
		return
	}
	if path == "/projects/test/collections" && r.Method == "POST" {
		if f.created {
			w.WriteHeader(409)
			return
		}
		var request map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&request)
		if len(request) != 2 || request["collectionName"] == nil || request["indexConfigs"] == nil {
			w.WriteHeader(400)
			return
		}
		f.created = true
		f.creates++
		if f.loseCreate {
			f.loseCreate = false
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(201)
		return
	}
	if path == "/projects/test/collections/items" && r.Method == "GET" {
		if !f.created {
			w.WriteHeader(404)
			return
		}
		meta := CollectionMetadata{CollectionName: "items", ProjectName: "test"}
		_ = json.NewEncoder(w).Encode(map[string]any{"collection": meta})
		return
	}
	var body struct {
		Docs []map[string]json.RawMessage `json:"docs"`
		IDs  []string                     `json:"ids"`
	}
	raw := new(bytes.Buffer)
	_, _ = raw.ReadFrom(r.Body)
	var public map[string]json.RawMessage
	if json.Unmarshal(raw.Bytes(), &public) != nil || len(public) != 1 || json.Unmarshal(raw.Bytes(), &body) != nil {
		w.WriteHeader(400)
		return
	}
	if len(body.Docs) == 1 {
		var id string
		_ = json.Unmarshal(body.Docs[0]["id"], &id)
		f.docs[id] = body.Docs[0]
	} else if len(body.IDs) == 1 {
		delete(f.docs, body.IDs[0])
	} else {
		w.WriteHeader(400)
		return
	}
	f.mutations++
	if f.afterMutation != nil {
		f.afterMutation()
	}
	if f.loseNext {
		f.loseNext = false
		w.WriteHeader(503)
		return
	}
	w.WriteHeader(202)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": "request accepted"})
}
func (f *workerFixture) worker(t *testing.T, initialize bool, configure ...func(*Worker)) (context.CancelFunc, <-chan error) {
	t.Helper()
	c, err := NewControl(f.server.URL, f.approved.Binding.ConnectionID, strings.Repeat("w", 32))
	if err != nil {
		t.Fatal(err)
	}
	c.Client = f.server.Client()
	worker := &Worker{Control: c, Secrets: fixtureSecrets{f}, StateDirectory: f.dir, Initialize: initialize, AllowInsecureSource: true, Transport: f.server.Client().Transport, PollInterval: 20 * time.Millisecond}
	for _, fn := range configure {
		fn(worker)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker cleanup timed out")
		}
	})
	return cancel, done
}
func stopWorker(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop")
	}
}
func (f *workerFixture) await(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		ok := check()
		f.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t.Fatalf("worker condition not reached: observation=%+v docs=%v", f.observation, f.docs)
}
func (f *workerFixture) sql(t *testing.T, sql string) {
	t.Helper()
	var p protocol.PostgresSelection
	_ = json.Unmarshal(f.approved.Binding.Source.Configuration, &p)
	if _, err := f.db.Exec(context.Background(), strings.ReplaceAll(sql, "TABLE", p.Schema+".items")); err != nil {
		t.Fatal(err)
	}
}
func (f *workerFixture) command(state string, resync bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.approved.Control.DesiredRevision++
	f.approved.Control.DesiredState = state
	f.approved.Control.AuthorityExpiresAt = time.Now().Add(time.Hour)
	if resync {
		f.approved.Control.ResyncRevision = f.approved.Control.DesiredRevision
	}
}
func (f *workerFixture) phaseIs(phase string) bool {
	return f.observation != nil && f.observation.Observation.Phase == phase && f.observation.Observation.DesiredRevision == f.approved.Control.DesiredRevision
}

func TestWorkerContinuousFixedTargetRestartPauseAndExplicitResync(t *testing.T) {
	f := newWorkerFixture(t, "continuous")
	cancel, done := f.worker(t, true)
	f.await(t, func() bool { return f.phaseIs("running") && len(f.docs) == 2 })
	f.mu.Lock()
	initialMutations := f.mutations
	f.mu.Unlock()
	stopWorker(t, cancel, done)
	cancel, done = f.worker(t, false)
	f.await(t, func() bool { return f.phaseIs("running") })
	f.sql(t, "UPDATE TABLE SET title='changed' WHERE pk=1;DELETE FROM TABLE WHERE pk=9007199254740993;INSERT INTO TABLE VALUES(2,'two','{}')")
	f.await(t, func() bool {
		return string(f.docs["1"]["title"]) == `"changed"` && f.docs["9007199254740993"] == nil && f.docs["2"] != nil
	})
	f.command("paused", false)
	f.await(t, func() bool { return f.phaseIs("paused") })
	f.mu.Lock()
	if f.creates != 1 || f.mutations != initialMutations+3 || f.docs["1"]["excluded"] != nil || string(f.docs["1"]["pk"]) != "1" {
		f.mu.Unlock()
		t.Fatal("restart rewrote baseline or leaked excluded data")
	}
	f.mu.Unlock()
	f.sql(t, "UPDATE TABLE SET pk=3,title='moved' WHERE pk=2")
	f.mu.Lock()
	if f.docs["3"] != nil {
		f.mu.Unlock()
		t.Fatal("paused worker applied a change")
	}
	f.mu.Unlock()
	f.command("running", false)
	f.await(t, func() bool { return f.docs["3"] != nil && f.docs["2"] == nil })
	// Keep replicating with cached authority while regional control is unavailable.
	f.mu.Lock()
	f.controlUnavailable = true
	f.mu.Unlock()
	f.sql(t, "UPDATE TABLE SET title='without-control' WHERE pk=1")
	f.await(t, func() bool { return string(f.docs["1"]["title"]) == `"without-control"` })
	f.mu.Lock()
	f.controlUnavailable = false
	f.mu.Unlock()
	f.command("paused", false)
	f.await(t, func() bool { return f.phaseIs("paused") })
	f.sql(t, "DELETE FROM TABLE WHERE pk=3;INSERT INTO TABLE VALUES(4,'fresh','{}')")
	f.command("running", true)
	f.await(t, func() bool { return f.docs["3"] == nil && f.docs["4"] != nil && f.phaseIs("running") })
	f.command("disconnected", false)
	f.await(t, func() bool { return f.phaseIs("disconnected") })
	stopWorker(t, cancel, done)
	f.mu.Lock()
	if f.creates != 1 || len(f.docs) != 2 {
		f.mu.Unlock()
		t.Fatal("disconnect did not preserve fixed target")
	}
	f.mu.Unlock()
	// Source credentials may be retired after receipt; terminal restart does not
	// recreate a target or require the source to be reachable.
	f.mu.Lock()
	f.secretsUnavailable = true
	f.approved.Control.DesiredRevision++
	f.approved.Control.AuthorityExpiresAt = time.Now().Add(-time.Minute)
	priorSequence := f.observation.Observation.Sequence
	f.mu.Unlock()
	cancel, done = f.worker(t, false)
	f.await(t, func() bool { return f.phaseIs("disconnected") && f.observation.Observation.Sequence > priorSequence })
	stopWorker(t, cancel, done)
}
func TestWorkerSnapshotCompletesOnceAndMissingVolumeFailsClosed(t *testing.T) {
	f := newWorkerFixture(t, "snapshot")
	cancel, done := f.worker(t, true)
	f.await(t, func() bool { return f.phaseIs("completed") && len(f.docs) == 2 })
	stopWorker(t, cancel, done)
	f.sql(t, "UPDATE TABLE SET title='after-cut' WHERE pk=1")
	cancel, done = f.worker(t, false)
	f.await(t, func() bool { return f.phaseIs("completed") })
	f.command("paused", false)
	f.await(t, func() bool { return f.phaseIs("paused") })
	f.command("running", false)
	f.await(t, func() bool { return f.phaseIs("completed") })
	f.mu.Lock()
	if string(f.docs["1"]["title"]) != `"one"` || f.mutations != 2 {
		f.mu.Unlock()
		t.Fatal("completed snapshot recaptured after restart")
	}
	f.mu.Unlock()
	stopWorker(t, cancel, done)
	if err := os.Rename(filepath.Join(f.dir, "source.db"), filepath.Join(f.dir, "source.retained")); err != nil {
		t.Fatal(err)
	}
	_, done = f.worker(t, false)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing source volume initialized")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing volume did not fail")
	}
	if _, err := os.Stat(filepath.Join(f.dir, "source.db")); !os.IsNotExist(err) {
		t.Fatal("missing volume recreated")
	}
	_, done = f.worker(t, true)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("reinitialization allowed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reinitialization did not fail")
	}
}

func TestWorkerUncertainWriteBlocksRestartResumeResyncAndDisconnect(t *testing.T) {
	f := newWorkerFixture(t, "continuous")
	cancel, done := f.worker(t, true)
	f.await(t, func() bool { return f.phaseIs("running") && len(f.docs) == 2 })
	f.mu.Lock()
	f.loseNext = true
	before := *f.observation.Observation.Position
	f.mu.Unlock()
	f.sql(t, "INSERT INTO TABLE VALUES(2,'partially-applied','{}')")
	f.await(t, func() bool {
		return f.docs["2"] != nil && f.phaseIs("action_required") && f.observation.Observation.ReasonCode == "target_outcome_uncertain"
	})
	stopWorker(t, cancel, done)
	f.mu.Lock()
	mutations := f.mutations
	sequence := f.observation.Observation.Sequence
	f.mu.Unlock()
	cancel, done = f.worker(t, false)
	f.await(t, func() bool { return f.observation.Observation.Sequence > sequence && f.phaseIs("action_required") })
	f.sql(t, "DELETE FROM TABLE WHERE pk=2;UPDATE TABLE SET title='later' WHERE pk=1")
	for _, command := range []struct {
		state  string
		resync bool
	}{{"paused", false}, {"running", false}, {"running", true}, {"disconnected", false}} {
		f.command(command.state, command.resync)
		f.await(t, func() bool {
			return f.phaseIs("action_required") && f.observation.Observation.DesiredRevision == f.approved.Control.DesiredRevision
		})
		f.mu.Lock()
		if f.mutations != mutations || f.creates != 1 || f.observation.Observation.ReasonCode != "target_outcome_uncertain" || *f.observation.Observation.Position != before {
			t.Error("command bypassed uncertainty or advanced source checkpoint")
		}
		f.mu.Unlock()
	}
	stopWorker(t, cancel, done)
}
func TestWorkerDoesNotAdoptExistingCollection(t *testing.T) {
	f := newWorkerFixture(t, "snapshot")
	f.mu.Lock()
	f.created = true
	f.mu.Unlock()
	cancel, done := f.worker(t, true)
	f.await(t, func() bool { return f.phaseIs("action_required") })
	stopWorker(t, cancel, done)
	f.mu.Lock()
	if f.mutations != 0 || f.creates != 0 {
		f.mu.Unlock()
		t.Fatal("unowned collection adopted")
	}
	f.mu.Unlock()
}

func TestWorkerUncertainCreationCannotAdoptVisibleCollectionOnRestart(t *testing.T) {
	f := newWorkerFixture(t, "snapshot")
	f.mu.Lock()
	f.loseCreate = true
	f.mu.Unlock()
	cancel, done := f.worker(t, true)
	f.await(t, func() bool {
		return f.phaseIs("action_required") && f.observation.Observation.ReasonCode == "target_outcome_uncertain"
	})
	stopWorker(t, cancel, done)
	f.mu.Lock()
	sequence := f.observation.Observation.Sequence
	f.mu.Unlock()
	cancel, done = f.worker(t, false)
	f.await(t, func() bool { return f.observation.Observation.Sequence > sequence && f.phaseIs("action_required") })
	f.command("running", true)
	f.await(t, func() bool { return f.phaseIs("action_required") })
	stopWorker(t, cancel, done)
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.created || f.creates != 1 || f.mutations != 0 || f.observation.Observation.ReasonCode != "target_outcome_uncertain" {
		t.Fatal("uncertain creation retried or visible collection adopted")
	}
}

func TestReportedUncertaintyRemainsBlockedEvenWithDurableAcceptanceProof(t *testing.T) {
	f := newWorkerFixture(t, "snapshot")
	cancel, done := f.worker(t, true)
	f.await(t, func() bool { return f.phaseIs("completed") })
	stopWorker(t, cancel, done)
	// A durability call may report failure even when its bytes survived. Once
	// uncertainty has been saved/reported, only operator recovery may clear it.
	j, err := OpenJournal(filepath.Join(f.dir, "connection.db"), protocol.Fingerprint(f.approved.Binding), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.UpdateData(func(raw json.RawMessage) (json.RawMessage, error) {
		var d workerData
		if err := json.Unmarshal(raw, &d); err != nil {
			return nil, err
		}
		d.Phase = "action_required"
		d.Reason = "target_outcome_uncertain"
		return json.Marshal(d)
	}); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	cancel, done = f.worker(t, false)
	f.await(t, func() bool { return f.phaseIs("action_required") })
	f.command("running", true)
	f.await(t, func() bool { return f.phaseIs("action_required") })
	stopWorker(t, cancel, done)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.mutations != 2 || f.observation.Observation.ReasonCode != "target_outcome_uncertain" {
		t.Fatal("reported uncertainty silently cleared")
	}
}

// Closing the actual retained handle during a successful HTTP response exercises
// the runner's fatal path even when target code maps the save error to uncertainty.
func TestWorkerStopsAfterRetainedHandleFailure(t *testing.T) {
	for _, file := range []string{"journal", "source"} {
		t.Run(file, func(t *testing.T) {
			f := newWorkerFixture(t, "continuous")
			_, done := f.worker(t, true, func(w *Worker) {
				f.afterMutation = func() {
					f.afterMutation = nil
					if file == "journal" {
						_ = w.journal.Close()
					} else {
						_ = w.source.Close()
					}
				}
			})
			select {
			case err := <-done:
				if !errors.Is(err, statefile.ErrUnavailable) {
					t.Fatalf("worker did not terminate on storage failure: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("worker retried a failed retained handle")
			}
			f.mu.Lock()
			if f.mutations != 1 {
				t.Errorf("sent %d mutations after storage failure", f.mutations)
			}
			f.mu.Unlock()
			j, err := OpenJournal(filepath.Join(f.dir, "connection.db"), protocol.Fingerprint(f.approved.Binding), false)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			state, err := j.Load()
			if err != nil {
				t.Fatal(err)
			}
			if file == "journal" && (state.AcceptedSequence != 0 || state.Pending == nil || state.Pending.Status != "sending") {
				t.Fatal("failed acceptance save lost pending transmission evidence")
			}
		})
	}
}
