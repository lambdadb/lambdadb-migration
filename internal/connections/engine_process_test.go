package connections

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
)

type engineProcessFixture struct {
	f           *workerFixture
	client      *http.Client
	regional    string
	workerToken string
	master      string
	ca          string
	command     int
}

func newEngineProcessFixture(t *testing.T, mode string) *engineProcessFixture {
	t.Helper()
	for _, key := range []string{"CONNECTION_TEST_TARGET_URL", "CONNECTION_TEST_CA_FILE", "CONNECTION_TEST_TLS_CERT", "CONNECTION_TEST_TLS_KEY", "CONNECTION_TEST_WORKER_BINARY", "CONNECTION_TEST_REGIONAL_BINARY"} {
		if os.Getenv(key) == "" {
			t.Skip("cross-repository component binaries and local TLS fixture required")
		}
	}
	f := newWorkerFixture(t, mode)
	root := x509.NewCertPool()
	pem, err := os.ReadFile(os.Getenv("CONNECTION_TEST_CA_FILE"))
	if err != nil || !root.AppendCertsFromPEM(pem) {
		t.Fatal("fixture CA invalid")
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: root, MinVersion: tls.VersionTLS12}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	f.approved.Binding.Target.URL = os.Getenv("CONNECTION_TEST_TARGET_URL")
	f.approved.Binding.Target.Collection = f.approved.Binding.ConnectionID
	f.approved.Binding.Target.SharedGateway = true
	f.approved.Binding.Target.ProjectID = "fixture-native-project"
	parsed, err := urlForFixture(f.approved.Binding.Target.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = parsed
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	master := strings.Repeat("local-operator-", 3)
	log, err := os.Create(filepath.Join(t.TempDir(), "regional.log"))
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Getenv("CONNECTION_TEST_REGIONAL_BINARY"), "-listen", addr, "-region", f.approved.Binding.Region, "-state", filepath.Join(t.TempDir(), "control.db"), "-tls-cert", os.Getenv("CONNECTION_TEST_TLS_CERT"), "-tls-key", os.Getenv("CONNECTION_TEST_TLS_KEY"))
	command.Env = append(os.Environ(), "CONNECTION_REGIONAL_CREDENTIAL="+master)
	command.Stdout = log
	command.Stderr = log
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait(); _ = log.Close() })
	p := &engineProcessFixture{f: f, client: client, regional: "https://" + addr, master: master, workerToken: protocol.WorkerToken(master, f.approved.Binding.Region, f.approved.Binding.ConnectionID), ca: os.Getenv("CONNECTION_TEST_CA_FILE")}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if p.publish() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("regional process did not become ready")
		}
		time.Sleep(30 * time.Millisecond)
	}
	return p
}
func urlForFixture(origin string) (string, error) {
	if !strings.HasPrefix(origin, "https://127.0.0.1:") {
		return "", fmt.Errorf("engine fixture must be localhost HTTPS")
	}
	return origin, nil
}
func (p *engineProcessFixture) publish() error {
	raw, _ := json.Marshal(p.f.approved)
	req, _ := http.NewRequest("PUT", p.regional+"/v1/connections/"+p.f.approved.Binding.ConnectionID+"/desired", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+p.master)
	req.Header.Set("Content-Type", "application/json")
	res, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 204 {
		return fmt.Errorf("command HTTP %d", res.StatusCode)
	}
	return nil
}
func (p *engineProcessFixture) start(t *testing.T, initialize bool) *exec.Cmd {
	t.Helper()
	p.command++
	args := []string{"-control-url", p.regional, "-connection-id", p.f.approved.Binding.ConnectionID, "-state-dir", p.f.dir, "-ca-file", p.ca, "-allow-environment-secrets", "-allow-insecure-source"}
	if initialize {
		args = append(args, "-initialize")
	}
	command := exec.Command(os.Getenv("CONNECTION_TEST_WORKER_BINARY"), args...)
	command.Env = append(os.Environ(), "CONNECTION_WORKER_CREDENTIAL="+p.workerToken, "SOURCE="+p.f.credentials, "TARGET=fixture-target-key", "AWS_REGION=ap-northeast-2")
	log, err := os.Create(filepath.Join(t.TempDir(), fmt.Sprintf("worker-%d.log", p.command)))
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout = log
	command.Stderr = log
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait(); _ = log.Close() })
	return command
}
func (p *engineProcessFixture) kill(t *testing.T, command *exec.Cmd) {
	t.Helper()
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("worker was not killed")
	}
}
func (p *engineProcessFixture) observed() (*protocol.Observed, error) {
	c, _ := NewControl(p.regional, p.f.approved.Binding.ConnectionID, p.workerToken)
	c.Client = p.client
	return c.Observed(context.Background())
}
func (p *engineProcessFixture) await(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.Now().Add(35 * time.Second)
	for time.Now().Before(until) {
		if condition() {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	observed, _ := p.observed()
	t.Fatalf("component condition timed out; observed=%+v", observed)
}
func (p *engineProcessFixture) phase(phase string) bool {
	o, e := p.observed()
	return e == nil && o != nil && o.Observation.Phase == phase && o.Observation.DesiredRevision == p.f.approved.Control.DesiredRevision && (phase != "completed" || o.Observation.CompletedResyncRevision == p.f.approved.Control.ResyncRevision)
}
func (p *engineProcessFixture) targetCall(t *testing.T, method, path string, body any) ([]byte, int) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, p.f.approved.Binding.Target.URL+path, bytes.NewReader(raw))
	req.Header.Set("x-api-key", "fixture-target-key")
	req.Header.Set("x-lambdadb-version", "v3")
	req.Header.Set("x-lambdadb-project-id", "fixture-native-project")
	req.Header.Set("Content-Type", "application/json")
	res, err := p.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return data, res.StatusCode
}
func (p *engineProcessFixture) fault(t *testing.T, body any) map[string]any {
	t.Helper()
	raw, status := p.targetCall(t, "POST", "/_fixture/control", body)
	if status != 200 {
		t.Fatalf("fixture control HTTP %d", status)
	}
	var result map[string]any
	_ = json.Unmarshal(raw, &result)
	return result
}
func (p *engineProcessFixture) fetch(t *testing.T, ids ...string) map[string]map[string]json.RawMessage {
	t.Helper()
	raw, status := p.targetCall(t, "POST", "/projects/test/collections/"+p.f.approved.Binding.Target.Collection+"/docs/fetch", map[string]any{"ids": ids, "consistentRead": true, "includeVectors": true})
	if status != 200 {
		return nil
	}
	var response struct {
		Docs []struct {
			Doc map[string]json.RawMessage `json:"doc"`
		} `json:"docs"`
	}
	if json.Unmarshal(raw, &response) != nil {
		t.Fatal("invalid supported Fetch response")
	}
	result := map[string]map[string]json.RawMessage{}
	for _, doc := range response.Docs {
		var id string
		_ = json.Unmarshal(doc.Doc["id"], &id)
		result[id] = doc.Doc
	}
	return result
}
func (p *engineProcessFixture) change(t *testing.T, state string, resync bool) {
	t.Helper()
	p.f.command(state, resync)
	if err := p.publish(); err != nil {
		t.Fatal(err)
	}
}

func TestEngineProcessUncertainWritesNeverResumeOrResend(t *testing.T) {
	for _, operation := range []string{"upsert", "delete", "lost-response"} {
		t.Run(operation, func(t *testing.T) {
			p := newEngineProcessFixture(t, "continuous")
			worker := p.start(t, true)
			p.await(t, func() bool { return p.phase("running") })
			before, _ := p.observed()
			position := *before.Observation.Position
			if operation == "lost-response" {
				p.fault(t, map[string]any{"lose_next": true})
			} else {
				p.fault(t, map[string]any{"hold_next": true})
			}
			if operation == "delete" {
				p.f.sql(t, "DELETE FROM TABLE WHERE pk=1")
			} else {
				p.f.sql(t, "UPDATE TABLE SET title='uncertain' WHERE pk=1")
			}
			if operation == "lost-response" {
				p.await(t, func() bool { return p.phase("action_required") })
			} else {
				p.await(t, func() bool { return p.fault(t, map[string]any{})["held"] == true })
			}
			p.kill(t, worker)
			worker = p.start(t, false)
			p.await(t, func() bool {
				o, _ := p.observed()
				return o != nil && o.Observation.Sequence > before.Observation.Sequence && o.Observation.ReasonCode == "target_outcome_uncertain"
			})
			stats := p.fault(t, map[string]any{"collection": p.f.approved.Binding.Target.Collection})
			mutations := stats["mutations"].(float64)
			if operation == "delete" {
				p.f.sql(t, "INSERT INTO TABLE VALUES(1,'later','{}')")
			} else {
				p.f.sql(t, "UPDATE TABLE SET title='later' WHERE pk=1")
			}
			for _, command := range []struct {
				state  string
				resync bool
			}{{"paused", false}, {"running", false}, {"running", true}, {"disconnected", false}} {
				p.change(t, command.state, command.resync)
				p.await(t, func() bool {
					o, _ := p.observed()
					return o != nil && o.Observation.DesiredRevision == p.f.approved.Control.DesiredRevision && o.Observation.ReasonCode == "target_outcome_uncertain"
				})
				o, _ := p.observed()
				if o.Observation.Phase != "action_required" || *o.Observation.Position != position {
					t.Fatal("uncertain write advanced checkpoint or completed command")
				}
				if p.fault(t, map[string]any{"collection": p.f.approved.Binding.Target.Collection})["mutations"].(float64) != mutations {
					t.Fatal("uncertain intent replayed or newer mutation submitted")
				}
			}
			if operation != "lost-response" {
				beforeRelease := p.fault(t, map[string]any{})["delayed_completed"].(float64)
				p.fault(t, map[string]any{"release": true})
				p.await(t, func() bool { return p.fault(t, map[string]any{})["delayed_completed"].(float64) > beforeRelease })
				// A delayed ordinary request can still arrive. The worker remains
				// blocked; it has not submitted a conflicting newer request.
				if p.fault(t, map[string]any{})["delayed_status"].(float64) != 202 {
					t.Fatal("ordinary delayed request did not exercise acceptance")
				}
			}
			p.kill(t, worker)
		})
	}
}

func TestEngineProcessSnapshotCursorAndStateLoss(t *testing.T) {
	p := newEngineProcessFixture(t, "snapshot")
	initialStats := p.fault(t, map[string]any{})
	before := initialStats["creates"].(float64)
	worker := p.start(t, true)
	p.await(t, func() bool { return p.phase("completed") })
	docs := p.fetch(t, "1", "9007199254740993")
	if len(docs) != 2 || docs["1"]["excluded"] != nil {
		t.Fatal("snapshot cursor recovery lost rows or leaked excluded data")
	}
	stats := p.fault(t, map[string]any{"collection": p.f.approved.Binding.Target.Collection})
	baseline := stats["mutations"].(float64)
	p.kill(t, worker)
	worker = p.start(t, false)
	p.f.sql(t, "DELETE FROM TABLE WHERE pk=1;INSERT INTO TABLE VALUES(2,'new-cut','{}')")
	p.change(t, "paused", false)
	p.await(t, func() bool { return p.phase("paused") })
	p.change(t, "running", false)
	p.await(t, func() bool { return p.phase("completed") })
	stats = p.fault(t, map[string]any{"collection": p.f.approved.Binding.Target.Collection})
	if stats["mutations"].(float64) != baseline {
		t.Fatal("completed snapshot was reimported")
	}
	p.change(t, "running", true)
	p.await(t, func() bool {
		docs := p.fetch(t, "1", "2")
		return p.phase("completed") && docs["1"] == nil && string(docs["2"]["title"]) == `"new-cut"`
	})
	p.kill(t, worker)
	source := filepath.Join(p.f.dir, "source.db")
	retained := source + ".retained"
	if err := os.Rename(source, retained); err != nil {
		t.Fatal(err)
	}
	stopped := p.start(t, false)
	exited := make(chan error, 1)
	go func() { exited <- stopped.Wait() }()
	select {
	case err := <-exited:
		if err == nil {
			t.Fatal("missing volume process succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("missing volume process did not fail")
	}
	if _, err := os.Stat(source); !os.IsNotExist(err) {
		t.Fatal("missing source state was recreated")
	}
	if err := os.Rename(retained, source); err != nil {
		t.Fatal(err)
	}
	p.change(t, "disconnected", false)
	worker = p.start(t, false)
	p.await(t, func() bool { return p.phase("disconnected") })
	if len(p.fetch(t, "2", "9007199254740993")) != 2 {
		t.Fatal("snapshot disconnect removed documents")
	}
	stats = p.fault(t, map[string]any{})
	if stats["creates"].(float64) != before+1 {
		t.Fatal("snapshot restart changed collection")
	}
	p.kill(t, worker)
}

func TestEngineProcessOrdinaryCDCAndSafeRestartKeepTheSameCollection(t *testing.T) {
	p := newEngineProcessFixture(t, "continuous")
	beforeCreates := p.fault(t, map[string]any{})["creates"].(float64)
	worker := p.start(t, true)
	p.await(t, func() bool { return p.phase("running") })
	initial, _ := p.observed()
	initialPosition := *initial.Observation.Position
	p.f.sql(t, "UPDATE TABLE SET title='changed' WHERE pk=1;DELETE FROM TABLE WHERE pk=9007199254740993;INSERT INTO TABLE VALUES(2,'two','{}')")
	p.await(t, func() bool {
		observed, _ := p.observed()
		docs := p.fetch(t, "1", "2", "9007199254740993")
		return observed != nil && observed.Observation.Position != nil && *observed.Observation.Position != initialPosition && string(docs["1"]["title"]) == `"changed"` && docs["2"] != nil && docs["9007199254740993"] == nil
	})
	beforeRestart, _ := p.observed()
	accepted := p.fault(t, map[string]any{"collection": p.f.approved.Binding.Target.Collection})["mutations"].(float64)
	p.kill(t, worker)
	worker = p.start(t, false)
	p.await(t, func() bool {
		observed, _ := p.observed()
		return p.phase("running") && observed != nil && observed.Observation.Sequence > beforeRestart.Observation.Sequence
	})
	if p.fault(t, map[string]any{"collection": p.f.approved.Binding.Target.Collection})["mutations"].(float64) != accepted {
		t.Fatal("safe restart rewrote accepted baseline")
	}
	p.change(t, "disconnected", false)
	p.await(t, func() bool { return p.phase("disconnected") })
	if len(p.fetch(t, "1", "2")) != 2 || p.fault(t, map[string]any{})["creates"].(float64) != beforeCreates+1 {
		t.Fatal("disconnect removed data or restart replaced collection")
	}
	p.kill(t, worker)
}
