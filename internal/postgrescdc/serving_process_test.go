//go:build unix

package postgrescdc

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServingProcessRecovery(t *testing.T) { runServingProcess(t, false) }
func TestLambdaDBLiveServing(t *testing.T) {
	if os.Getenv("CDC_TEST_LIVE") != "1" {
		t.Skip("opt-in live target required")
	}
	runServingProcess(t, true)
}
func runServingProcess(t *testing.T, live bool) {
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
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	cert := control.server.Certificate()
	private, err := x509.MarshalPKCS8PrivateKey(control.server.TLS.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	args := []string{"-search-listen", address, "-search-tls-cert", certPath, "-search-tls-key", keyPath}
	client := control.server.Client()
	client.Timeout = 3 * time.Second
	var children []*workerProcess
	if live {
		t.Cleanup(func() {
			names := map[string]bool{}
			for _, child := range children {
				child.mu.Lock()
				for name := range child.seen {
					names[name] = true
				}
				child.mu.Unlock()
			}
			if store, err := OpenStore(f.path); err == nil {
				if e, err := store.Execution(); err == nil && e.Owner.Name != "" {
					names[e.Owner.Name] = true
				}
				store.Close()
			}
			for name := range names {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := target.Client.Collection(name).Delete(ctx)
				cancel()
				if err != nil {
					t.Errorf("serving cleanup failed: %s", name)
				}
			}
		})
	}
	startChild := func() *workerProcess {
		child := startManagedSearchProcess(t, binary, f, control, key, certPath, args)
		children = append(children, child)
		return child
	}
	// The regional search listener is part of the actual worker process. There is
	// no central routing dependency or independent stale-observation cache.
	call := func(term, token string) (int, *SearchResponse) {
		t.Helper()
		body, _ := json.Marshal(SearchRequest{Field: "title", Term: term, Limit: 100})
		r, _ := http.NewRequest("POST", "https://"+address+"/v1/bindings/"+f.c.BindingID+"/search", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(r)
		if err != nil {
			return 0, nil
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		if response.StatusCode != 200 {
			if strings.Contains(string(data), "baseline") || strings.Contains(string(data), "cedar") {
				t.Fatal("unavailable response leaked documents")
			}
			return response.StatusCode, nil
		}
		var result SearchResponse
		if json.Unmarshal(data, &result) != nil {
			t.Fatal("bad search response")
		}
		return 200, &result
	}
	searchToken := "synthetic-read-only-search-at-least-32-bytes"
	await := func(child *workerProcess, term string, want map[string]Row) *SearchResponse {
		t.Helper()
		timeout := time.After(3 * time.Minute)
		for {
			code, result := call(term, searchToken)
			if code == 200 {
				actual := map[string]any{}
				for _, doc := range result.Docs {
					actual[doc["id"].(string)] = doc
				}
				expected := map[string]any{}
				for id, row := range want {
					expected[id] = document(id, row)
				}
				if equalJSON(actual, expected) {
					return result
				}
			}
			select {
			case <-child.done:
				t.Fatalf("serving worker exited: %s", child.stderr.String())
			case <-timeout:
				t.Fatalf("search never converged for %s (last status %d)", term, code)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	firstChild := startChild()
	first := control.wait(t, firstChild, func(o Observation) bool { return o.Phase == "active" })
	baselineRows := sourceRows(t, f)
	initial := await(firstChild, "baseline", map[string]Row{"1": baselineRows["1"]})
	t.Log("initial committed token search verified")
	if initial.Epoch != first.Epoch || initial.ValidatedLSN == "" {
		t.Fatal("search not bound to current execution")
	}
	if code, _ := call("baseline", control.token); code != 401 {
		t.Fatal("worker credential can search", code)
	}
	// Query visibility must include updates, old-token removal and deleted rows.
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET title='cedar' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=2; INSERT INTO "+f.c.Schema+".items VALUES(3,'cedar','inserted','{}')")
	changed := sourceRows(t, f)
	await(firstChild, "cedar", map[string]Row{"1": changed["1"], "3": changed["3"]})
	await(firstChild, "baseline", map[string]Row{})
	await(firstChild, "deleted", map[string]Row{})
	t.Log("insert/update/delete and old-token query results verified")
	control.set(2, "paused", time.Now().Add(time.Hour))
	control.wait(t, firstChild, func(o Observation) bool { return o.DesiredRevision == 2 && o.Phase == "paused" })
	if code, _ := call("cedar", searchToken); code != 503 {
		t.Fatal("paused search available", code)
	}
	if live {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		docs, total, err := target.SearchToken(ctx, first.Generation, "title", "cedar")
		cancel()
		if err != nil || total != 2 || len(docs) != 2 {
			t.Fatal("direct project-key boundary could not be verified")
		}
		t.Log("direct project-key access still bypasses the paused managed route; protected target credentials remain required")
	}

	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET id=9,title='pine' WHERE id=1; DELETE FROM "+f.c.Schema+".items WHERE id=3")
	control.set(3, "running", time.Now().Add(time.Hour))
	resumed := control.wait(t, firstChild, func(o Observation) bool {
		return o.DesiredRevision == 3 && o.Phase == "active" && o.Epoch > first.Epoch
	})
	rows := sourceRows(t, f)
	await(firstChild, "pine", map[string]Row{"9": rows["9"]})
	await(firstChild, "cedar", map[string]Row{})
	t.Log("pause/resume selected a replacement without exposing retired results")
	control.unavailable(true)
	killManaged(t, firstChild)
	waitInactive(t, f)
	if code, _ := call("pine", searchToken); code == 200 {
		t.Fatal("dead worker served a stale route")
	}
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET title='maple' WHERE id=9")
	recovered := startChild()
	finalRows := sourceRows(t, f)
	final := await(recovered, "maple", map[string]Row{"9": finalRows["9"]})
	if final.Epoch <= resumed.Epoch {
		t.Fatal("restart reused serving epoch")
	}
	await(recovered, "pine", map[string]Row{})
	t.Log("committed search recovered while regional delivery remained unavailable")
	control.unavailable(false)
	control.wait(t, recovered, func(o Observation) bool {
		return o.DesiredRevision == 3 && o.Phase == "active" && o.Epoch == final.Epoch
	})
	control.set(4, "running", time.Now().Add(3*time.Second))
	control.wait(t, recovered, func(o Observation) bool { return o.DesiredRevision == 4 && o.Phase == "expired" })
	if code, _ := call("maple", searchToken); code != 503 {
		t.Fatal("expired search available", code)
	}
	recovered.stop(t)
	waitInactive(t, f)
	t.Log("committed query visibility, old-token/delete removal, pause/resume, SIGKILL and offline restart serving gates verified")
}
