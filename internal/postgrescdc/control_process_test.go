//go:build unix

package postgrescdc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

type processControl struct {
	mu             sync.Mutex
	desired        Approved
	last           Observation
	offline        bool
	server         *httptest.Server
	token          string
	regionalURL    string
	regionalClient *http.Client
	master         string
}

func newProcessControl(t *testing.T, c Config) *processControl {
	t.Helper()
	m := &processControl{desired: approvedTest(c), token: "synthetic-binding-token-at-least-32-bytes"}
	m.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+m.token {
			w.WriteHeader(401)
			return
		}
		if m.offline {
			w.WriteHeader(503)
			return
		}
		if m.regionalURL != "" {
			request, _ := http.NewRequestWithContext(r.Context(), r.Method, m.regionalURL+r.URL.Path, r.Body)
			request.Header = r.Header.Clone()
			response, err := m.regionalClient.Do(request)
			if err != nil {
				w.WriteHeader(503)
				return
			}
			defer response.Body.Close()
			w.WriteHeader(response.StatusCode)
			_, _ = io.Copy(w, response.Body)
			return
		}
		if r.Method == "GET" {
			_ = json.NewEncoder(w).Encode(m.desired)
			return
		}
		var o Observation
		if json.NewDecoder(r.Body).Decode(&o) != nil || o.Sequence <= m.last.Sequence {
			w.WriteHeader(409)
			return
		}
		m.last = o
		w.WriteHeader(204)
	}))
	t.Cleanup(m.server.Close)
	if os.Getenv("CDC_TEST_REGIONAL_BINARY") != "" {
		m.startRegional(t)
	}
	return m
}
func (m *processControl) set(rev uint64, state string, expires time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.desired.Control.DesiredRevision = rev
	m.desired.Control.DesiredState = state
	m.desired.Control.AuthorityExpiresAt = expires
	if m.regionalURL != "" {
		m.publish()
	}
}
func (m *processControl) unavailable(v bool) { m.mu.Lock(); defer m.mu.Unlock(); m.offline = v }
func (m *processControl) status() Observation {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.regionalURL != "" {
		request, _ := http.NewRequest("GET", m.regionalURL+"/v1/bindings/"+m.desired.Binding.BindingID+"/observed", nil)
		request.Header.Set("Authorization", "Bearer "+m.master)
		response, err := m.regionalClient.Do(request)
		if err == nil {
			defer response.Body.Close()
			var result struct {
				Observation Observation `json:"observation"`
			}
			if response.StatusCode == 200 && json.NewDecoder(response.Body).Decode(&result) == nil {
				return result.Observation
			}
		}
	}
	return m.last
}
func (m *processControl) wait(t *testing.T, p *workerProcess, predicate func(Observation) bool) Observation {
	t.Helper()
	deadline := time.NewTimer(2 * time.Minute)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			o := m.status()
			if predicate(o) {
				return o
			}
		case <-p.done:
			t.Fatalf("managed worker exited: %v %s", p.err, p.stderr.String())
		case <-deadline.C:
			t.Fatalf("managed status timeout: %+v", m.status())
		}
	}
}
func startManagedProcess(t *testing.T, binary string, f *fixture, m *processControl, key, caPath string) *workerProcess {
	return startManagedSearchProcess(t, binary, f, m, key, caPath, nil)
}
func startManagedSearchProcess(t *testing.T, binary string, f *fixture, m *processControl, key, caPath string, extra []string) *workerProcess {
	t.Helper()
	p := &workerProcess{progress: make(chan Progress, 128), done: make(chan struct{}), seen: map[string]bool{}}
	args := append([]string{"-control-url", m.server.URL, "-binding-id", f.c.BindingID, "-state", f.path}, extra...)
	p.cmd = exec.Command(binary, args...)
	p.cmd.Env = append(os.Environ(), "TEST_DSN="+f.dsn, "TEST_KEY="+key, "CDC_CONTROL_TOKEN="+m.token, "CDC_TEST_CA_FILE="+caPath, "CDC_SEARCH_TOKEN=synthetic-read-only-search-at-least-32-bytes")
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
			var v Progress
			if json.Unmarshal(scanner.Bytes(), &v) == nil && v.Generation != "" {
				p.mu.Lock()
				p.seen[v.Generation] = true
				p.mu.Unlock()
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
func killManaged(t *testing.T, p *workerProcess) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("managed SIGKILL timeout")
	}
	exit, ok := p.err.(*exec.ExitError)
	if !ok {
		t.Fatal("expected SIGKILL")
	}
	s, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || s.Signal() != syscall.SIGKILL {
		t.Fatal("not SIGKILL")
	}
}

func TestManagedControlProcessRecovery(t *testing.T) { runManagedControlRecovery(t, false) }
func TestLambdaDBLiveManagedControl(t *testing.T) {
	if os.Getenv("CDC_TEST_LIVE") != "1" {
		t.Skip("opt-in live target credentials required")
	}
	runManagedControlRecovery(t, true)
}
func runManagedControlRecovery(t *testing.T, live bool) {
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
			t.Fatal("live key required")
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
	caPath := filepath.Join(t.TempDir(), "control-ca.pem")
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	var children []*workerProcess
	if live {
		t.Cleanup(func() {
			for _, child := range children {
				child.mu.Lock()
				for name := range child.seen {
					names[name] = true
				}
				child.mu.Unlock()
			}
			for name := range names {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := target.Client.Collection(name).Delete(ctx)
				cancel()
				if err != nil {
					t.Errorf("managed test generation cleanup failed: %s", name)
				}
			}
		})
	}
	child := startManagedProcess(t, binary, f, control, key, caPath)
	children = append(children, child)
	first := control.wait(t, child, func(o Observation) bool { return o.Phase == "active" && o.DesiredRevision == 1 })
	names[first.Generation] = true
	// Renewal is a control revision, never a new binding, slot or generation.
	renewed := time.Now().UTC().Add(20 * time.Minute)
	control.set(2, "running", renewed)
	second := control.wait(t, child, func(o Observation) bool { return o.DesiredRevision == 2 && o.Phase == "active" })
	if second.Epoch != first.Epoch || second.Generation != first.Generation {
		t.Fatal("renewal restarted healthy execution")
	}
	control.set(1, "paused", renewed)
	before := second.Sequence
	control.wait(t, child, func(o Observation) bool { return o.Sequence > before && o.DesiredRevision == 2 && o.Phase == "active" })
	control.set(3, "paused", renewed)
	paused := control.wait(t, child, func(o Observation) bool { return o.DesiredRevision == 3 && o.Phase == "paused" })
	waitInactive(t, f)
	// A completed pause must have closed replication before reporting completion.
	var active bool
	if err := f.sql.QueryRow(context.Background(), `SELECT active FROM pg_replication_slots WHERE slot_name=$1`, f.c.Slot).Scan(&active); err != nil || active {
		t.Fatal("pause acknowledged while capture active", err)
	}
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=9,title='resumed',metadata='{}' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(3,'new','body','null')")
	// Resume rebuilds from the same regional row state and retained WAL.
	control.set(4, "running", renewed)
	resumed := control.wait(t, child, func(o Observation) bool { return o.DesiredRevision == 4 && o.Phase == "active" })
	names[resumed.Generation] = true
	if resumed.Epoch <= paused.Epoch || resumed.Generation == first.Generation {
		t.Fatal("resume revived stopped generation")
	}
	// Central outage is represented by absence of further commands. Regional
	// delivery can fail too: source batches and restart use cached authority.
	control.unavailable(true)
	killManaged(t, child)
	waitInactive(t, f)
	baseline, execution := readProcessState(t, f)
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET title='offline-restart' WHERE id=9; DELETE FROM "+f.c.Schema+".items WHERE id=3; INSERT INTO "+f.c.Schema+".items VALUES(2,'reinsert','latest','{}')")
	recovered := startManagedProcess(t, binary, f, control, key, caPath)
	children = append(children, recovered)
	// Wait for actual target equality while control remains unavailable. No
	// progress/observation response can make this assertion succeed by itself.
	deadline := time.Now().Add(2 * time.Minute)
	var generation string
	offlineVerified := false
	for time.Now().Before(deadline) {
		recovered.mu.Lock()
		for name := range recovered.seen {
			generation = name
		}
		recovered.mu.Unlock()
		if generation != "" {
			if mock != nil {
				mock.mu.Lock()
				want := map[string]any{}
				for id, row := range sourceRows(t, f) {
					want[id] = document(id, row)
				}
				ok := equalJSON(want, mock.docs[generation])
				mock.mu.Unlock()
				if ok {
					offlineVerified = true
					break
				}
			} else {
				assertLiveRows(t, target, generation, sourceRows(t, f))
				offlineVerified = true
				break
			}
		}
		select {
		case <-recovered.done:
			t.Fatalf("offline restart stopped: %s", recovered.stderr.String())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !offlineVerified {
		t.Fatal("offline recovery did not reach full source equality")
	}
	names[generation] = true
	control.unavailable(false)
	reconciled := control.wait(t, recovered, func(o Observation) bool {
		return o.DesiredRevision == 4 && o.Phase == "active" && o.Epoch > execution.Owner.Epoch && o.Sequence > resumed.Sequence
	})
	if reconciled.Generation == resumed.Generation {
		t.Fatal("restart reused uncertain generation")
	}
	// A short grant expires without a central renewal; a later grant must create
	// a new execution epoch rather than revive the expired writer.
	control.set(5, "running", time.Now().Add(3*time.Second))
	expired := control.wait(t, recovered, func(o Observation) bool { return o.DesiredRevision == 5 && o.Phase == "expired" })
	waitInactive(t, f)
	control.set(6, "running", renewed)
	final := control.wait(t, recovered, func(o Observation) bool {
		return o.DesiredRevision == 6 && o.Phase == "active" && o.Epoch > expired.Epoch
	})
	names[final.Generation] = true
	recovered.stop(t)
	waitInactive(t, f)
	state, e := readProcessState(t, f)
	if state.LSN < baseline.LSN || state.SystemID != baseline.SystemID || state.RelationID != baseline.RelationID || !equalJSON(state.Rows, sourceRows(t, f)) || e.Active == nil || e.Active.Epoch != final.Epoch {
		t.Fatal("managed recovery lost or regressed source state")
	}
	if mock != nil {
		mock.assertRows(t, final.Generation, state.Rows)
	} else {
		assertLiveRows(t, target, final.Generation, state.Rows)
	}
	t.Log("renewal, stale command rejection, pause/resume, cached SIGKILL recovery, authority expiration and regional reconciliation verified")
}

// Optional cross-repository black-box validation uses the backend-owned regional
// executable supplied explicitly by the caller, never a private checkout path.
func (m *processControl) startRegional(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	cert := m.server.Certificate()
	key, err := x509.MarshalPKCS8PrivateKey(m.server.TLS.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	m.master = "synthetic-regional-operator-at-least-32-bytes"
	m.regionalURL = "https://" + address
	m.regionalClient = m.server.Client()
	m.regionalClient.Timeout = 2 * time.Second
	mac := hmac.New(sha256.New, []byte(m.master))
	_, _ = mac.Write([]byte("cdc-worker-v1\x00" + m.desired.Binding.TargetRegion + "\x00" + m.desired.Binding.BindingID))
	m.token = hex.EncodeToString(mac.Sum(nil))
	command := exec.Command(os.Getenv("CDC_TEST_REGIONAL_BINARY"), "-listen", address, "-region", m.desired.Binding.TargetRegion, "-state", filepath.Join(dir, "control.db"), "-tls-cert", certPath, "-tls-key", keyPath)
	command.Env = append(os.Environ(), "CDC_REGIONAL_CREDENTIAL="+m.master)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	var exitErr error
	go func() { exitErr = command.Wait(); close(exited) }()
	t.Cleanup(func() {
		select {
		case <-exited:
			return
		default:
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			<-exited
			t.Error("backend regional process did not shut down")
		}
	})
	ready := false
	// A new executable can start slowly under cold build/OS inspection load.
	// This is a test readiness budget, not a production timeout or lease change.
	for end := time.Now().Add(30 * time.Second); time.Now().Before(end); {
		select {
		case <-exited:
			t.Fatalf("backend regional process exited before readiness: %v; %s", exitErr, stderr.String())
		default:
		}
		request, _ := http.NewRequest("GET", m.regionalURL+"/v1/bindings/"+m.desired.Binding.BindingID+"/desired", nil)
		request.Header.Set("Authorization", "Bearer "+m.token)
		response, err := m.regionalClient.Do(request)
		if err == nil {
			response.Body.Close()
			ready = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		t.Fatal("backend regional service not ready within the test startup budget")
	}
	if m.publish() != 204 {
		t.Fatal("backend rejected initial worker protocol")
	}
	t.Log("using actual backend regional executable over TLS")
}
func (m *processControl) publish() int {
	data, _ := json.Marshal(m.desired)
	request, _ := http.NewRequest("PUT", m.regionalURL+"/v1/bindings/"+m.desired.Binding.BindingID+"/desired", bytes.NewReader(data))
	request.Header.Set("Authorization", "Bearer "+m.master)
	response, err := m.regionalClient.Do(request)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	return response.StatusCode
}
