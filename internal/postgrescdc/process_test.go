//go:build unix

package postgrescdc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

// The server deliberately survives child death. Each child uses the real SDK
// over TLS and cannot recover by borrowing target state from parent memory.
type processTarget struct {
	mu     sync.Mutex
	docs   map[string]map[string]map[string]any
	server *httptest.Server
	caPath string
}

func newProcessTarget(t *testing.T) *processTarget {
	t.Helper()
	m := &processTarget{docs: map[string]map[string]map[string]any{}}
	m.server = httptest.NewTLSServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.server.Close)
	m.caPath = filepath.Join(t.TempDir(), "target-ca.pem")
	if err := os.WriteFile(m.caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: m.server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	return m
}
func (m *processTarget) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	m.mu.Lock()
	defer m.mu.Unlock()
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "projects" || parts[2] != "collections" {
		http.NotFound(w, r)
		return
	}
	decode := func(v any) bool { d := json.NewDecoder(r.Body); d.UseNumber(); return d.Decode(v) == nil }
	if len(parts) == 3 && r.Method == http.MethodPost {
		var body struct {
			Name string `json:"collectionName"`
		}
		if !decode(&body) || body.Name == "" {
			w.WriteHeader(400)
			return
		}
		if _, ok := m.docs[body.Name]; ok {
			w.WriteHeader(409)
			return
		}
		m.docs[body.Name] = map[string]map[string]any{}
		w.WriteHeader(201)
		_ = json.NewEncoder(w).Encode(map[string]any{"collection": map[string]any{"collectionName": body.Name, "description": "", "tags": map[string]string{}, "defaultBranchName": "main", "snapshotRetentionInDays": 7, "createdAt": 1700000000000}})
		return
	}
	if len(parts) == 4 && (r.Method == "GET" || r.Method == "DELETE") {
		name := parts[3]
		if _, ok := m.docs[name]; !ok {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"message":"collection not found"}`))
			return
		}
		if r.Method == "DELETE" {
			delete(m.docs, name)
			_, _ = w.Write([]byte(`{"message":"deleted"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"collection": map[string]any{"collectionName": name, "description": "", "tags": map[string]string{}, "defaultBranchName": "main", "snapshotRetentionInDays": 7, "createdAt": 1700000000000}})
		return
	}
	if len(parts) < 5 {
		http.NotFound(w, r)
		return
	}
	name := parts[3]
	docs, ok := m.docs[name]
	if !ok {
		w.WriteHeader(404)
		return
	}
	if len(parts) == 5 && parts[4] == "query" && r.Method == "POST" {
		var body struct {
			Query struct {
				QueryString struct {
					Query        string `json:"query"`
					DefaultField string `json:"defaultField"`
					SkipSyntax   bool   `json:"skipSyntax"`
				} `json:"queryString"`
			} `json:"query"`
			ConsistentRead bool `json:"consistentRead"`
			Size           int  `json:"size"`
		}
		if !decode(&body) || body.ConsistentRead || !body.Query.QueryString.SkipSyntax || body.Size != MaxServingRows {
			w.WriteHeader(400)
			return
		}
		hits := []map[string]any{}
		for _, doc := range docs {
			value, _ := doc[body.Query.QueryString.DefaultField].(string)
			for _, word := range strings.Fields(strings.ToLower(value)) {
				if word == body.Query.QueryString.Query {
					hits = append(hits, map[string]any{"collection": name, "doc": doc, "score": 1.0})
					break
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total": len(hits), "took": 0, "docs": hits, "isDocsInline": true, "maxScore": 1.0})
		return
	}
	if len(parts) == 6 && parts[4] == "docs" && parts[5] == "upsert" {
		var body struct {
			Docs []map[string]any `json:"docs"`
		}
		if !decode(&body) {
			w.WriteHeader(400)
			return
		}
		for _, doc := range body.Docs {
			id, ok := doc["id"].(string)
			if !ok {
				w.WriteHeader(400)
				return
			}
			docs[id] = doc
		}
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "accepted"})
		return
	}
	if len(parts) == 6 && parts[4] == "docs" && parts[5] == "delete" {
		var body struct {
			IDs []string `json:"ids"`
		}
		if !decode(&body) {
			w.WriteHeader(400)
			return
		}
		for _, id := range body.IDs {
			delete(docs, id)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "deleted"})
		return
	}
	var ids []string
	if len(parts) == 6 && parts[4] == "docs" && parts[5] == "fetch" {
		var body struct {
			IDs []string `json:"ids"`
		}
		if !decode(&body) {
			w.WriteHeader(400)
			return
		}
		ids = body.IDs
	} else if len(parts) == 5 && parts[4] == "docs" && r.Method == http.MethodGet {
		for id := range docs {
			ids = append(ids, id)
		}
	} else {
		http.NotFound(w, r)
		return
	}
	result := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		if doc, ok := docs[id]; ok {
			result = append(result, map[string]any{"collection": name, "doc": doc})
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"total": len(result), "took": 0, "docs": result, "isDocsInline": true})
}
func (m *processTarget) assertRows(t *testing.T, generation string, expected map[string]Row) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	want := map[string]any{}
	for id, row := range expected {
		want[id] = document(id, row)
	}
	if !equalJSON(want, m.docs[generation]) {
		t.Fatal("process recovery differs from full source PK/field set")
	}
}

func buildProcessWorker(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "cdc-worker")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-tags", "cdctest", "-o", binary, "./cmd/cdc-worker")
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("diagnostic worker build: %v\n%s", err, out)
	}
	return binary
}

type workerProcess struct {
	cmd      *exec.Cmd
	progress chan Progress
	done     chan struct{}
	err      error
	boundary string
	stderr   bytes.Buffer
	mu       sync.Mutex
	seen     map[string]bool
}

func startProcess(t *testing.T, binary string, f *fixture, caPath, key, point string) *workerProcess {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "binding.json")
	data, err := json.Marshal(f.c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	p := &workerProcess{progress: make(chan Progress, 128), done: make(chan struct{}), boundary: filepath.Join(t.TempDir(), "boundary"), seen: map[string]bool{}}
	p.cmd = exec.Command(binary, "-config", configPath, "-state", f.path)
	p.cmd.Env = append(os.Environ(), "TEST_DSN="+f.dsn, "TEST_KEY="+key, "CDC_TEST_STOP_AT="+point, "CDC_TEST_BOUNDARY_FILE="+p.boundary, "CDC_TEST_CA_FILE="+caPath)
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = &p.stderr
	if err = p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			var progress Progress
			if json.Unmarshal(scanner.Bytes(), &progress) == nil {
				if progress.Generation != "" {
					p.mu.Lock()
					p.seen[progress.Generation] = true
					p.mu.Unlock()
				}
				p.progress <- progress
			}
		}
	}()
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	})
	return p
}
func (p *workerProcess) phase(t *testing.T, phase string) Progress {
	t.Helper()
	timer := time.NewTimer(5 * time.Minute)
	defer timer.Stop()
	for {
		select {
		case progress := <-p.progress:
			if progress.Phase == phase {
				return progress
			}
		case <-p.done:
			t.Fatalf("worker exited before %s: %v; %s", phase, p.err, p.stderr.String())
		case <-timer.C:
			t.Fatalf("worker phase timeout: %s", phase)
		}
	}
}
func (p *workerProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		if p.err != nil {
			t.Fatalf("graceful process stop: %v; %s", p.err, p.stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process did not stop")
	}
}
func (p *workerProcess) killAt(t *testing.T, point string) {
	t.Helper()
	timeout := time.After(30 * time.Second)
	for {
		data, err := os.ReadFile(p.boundary)
		if err == nil && string(data) == point {
			break
		}
		select {
		case <-p.done:
			t.Fatalf("worker exited before boundary %s: %v; %s", point, p.err, p.stderr.String())
		case <-timeout:
			t.Fatalf("boundary timeout: %s", point)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("SIGKILL did not reap worker")
	}
	exit, ok := p.err.(*exec.ExitError)
	if !ok {
		t.Fatalf("worker was not killed: %v", p.err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("worker did not die from SIGKILL")
	}
}
func waitInactive(t *testing.T, f *fixture) {
	t.Helper()
	timeout := time.Now().Add(5 * time.Second)
	for {
		var active bool
		err := f.sql.QueryRow(context.Background(), `SELECT COALESCE(bool_or(active),false) FROM pg_replication_slots WHERE slot_name=$1`, f.c.Slot).Scan(&active)
		if err != nil {
			t.Fatal(err)
		}
		if !active {
			return
		}
		if time.Now().After(timeout) {
			t.Fatal("dead process still owns source slot")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func readProcessState(t *testing.T, f *fixture) (*State, *Execution) {
	t.Helper()
	store, err := OpenStore(f.path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	e, err := store.Execution()
	if err != nil {
		t.Fatal(err)
	}
	return state, e
}

func TestPostgresProcessCrashRecovery(t *testing.T) {
	if os.Getenv("CDC_TEST_POSTGRES_DSN") == "" {
		t.Skip("isolated PostgreSQL required")
	}
	binary := buildProcessWorker(t)
	for _, point := range []string{"snapshot_row", "after_apply", "after_save", "rebuild_row", "before_apply", "before_activate", "activation_write", "after_activate"} {
		t.Run(point, func(t *testing.T) {
			f := newFixture(t)
			target := newProcessTarget(t)
			f.c.TargetURL = target.server.URL
			var baseline *State
			var previous *Selection
			if point != "snapshot_row" {
				seed := startProcess(t, binary, f, target.caPath, "synthetic-test-key", "")
				_ = seed.phase(t, "active")
				seed.stop(t)
				waitInactive(t, f)
				var e *Execution
				baseline, e = readProcessState(t, f)
				previous = e.Active
				execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=9,title='moved',metadata='{}' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(2,'reinsert','newer','null'),(3,'insert','body','{}')")
			}
			crashing := startProcess(t, binary, f, target.caPath, "synthetic-test-key", point)
			crashing.killAt(t, point)
			waitInactive(t, f)
			state, killed := readProcessState(t, f)
			if point == "snapshot_row" {
				if state != nil || killed.Active != nil {
					t.Fatal("incomplete snapshot became active/recoverable")
				}
			} else {
				if point == "after_activate" {
					if killed.Active == nil || killed.Active.Epoch != killed.Owner.Epoch || killed.Active.Epoch <= previous.Epoch {
						t.Fatal("committed activation was lost")
					}
				} else if killed.Active == nil || *killed.Active != *previous {
					t.Fatal("unverified generation replaced previous selection")
				}
				if point == "after_apply" || point == "before_apply" || point == "rebuild_row" {
					if state.LSN < baseline.LSN || !equalJSON(state.Rows, baseline.Rows) {
						t.Fatal("checkpoint advanced before complete apply")
					}
				}
				if point == "after_save" {
					if state.LSN <= baseline.LSN || state.Rows["9"] == nil {
						t.Fatal("saved transaction was not recovered")
					}
					var confirmed string
					if err := f.sql.QueryRow(context.Background(), `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, f.c.Slot).Scan(&confirmed); err != nil {
						t.Fatal(err)
					}
					lsn, err := pglogrepl.ParseLSN(confirmed)
					if err != nil || uint64(lsn) >= state.LSN {
						t.Fatal("source acknowledgement crossed the kill boundary")
					}
				}
				if (point == "before_activate" || point == "activation_write" || point == "after_activate") && killed.Owner.VerifiedLSN == 0 {
					t.Fatal("activation fault did not follow verification")
				}
			}
			execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=10,title='final',metadata='{}' WHERE id IN(1,9); DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(2,'after restart','newest','null')")
			execSQL(t, f.sql, "BEGIN; INSERT INTO "+f.c.Schema+".items VALUES(4,'aborted','bad','{}'); ROLLBACK")
			recovered := startProcess(t, binary, f, target.caPath, "synthetic-test-key", "")
			progress := recovered.phase(t, "active")
			recovered.stop(t)
			waitInactive(t, f)
			state, current := readProcessState(t, f)
			expected := sourceRows(t, f)
			if !equalJSON(state.Rows, expected) || current.Active == nil || current.Active.Epoch != progress.Epoch || current.Active.Generation != progress.Generation || current.Owner.Epoch <= killed.Owner.Epoch {
				t.Fatal("process restart regressed source or selection")
			}
			target.assertRows(t, progress.Generation, expected)
			store, err := OpenStore(f.path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.activate(killed.Owner, killed.Owner.ExpectedActiveEpoch); err == nil {
				t.Fatal("delayed old activation accepted")
			}
			_ = store.Close()
			if killed.Owner.Name != "" {
				target.mu.Lock()
				delete(target.docs[killed.Owner.Name], "2")
				target.mu.Unlock()
				target.assertRows(t, progress.Generation, expected)
			}
			t.Logf("SIGKILL at %s recovered into epoch %d with exact PK/field equality", point, progress.Epoch)
		})
	}
}
