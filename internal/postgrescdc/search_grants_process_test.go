//go:build unix

package postgrescdc

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestSearchGrantsProcess(t *testing.T) { runSearchGrantsProcess(t, false) }
func TestLambdaDBLiveSearchGrants(t *testing.T) {
	if os.Getenv("CDC_TEST_LIVE") != "1" {
		t.Skip("opt-in real target required")
	}
	runSearchGrantsProcess(t, true)
}
func runSearchGrantsProcess(t *testing.T, live bool) {
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
	privateTLS, err := x509.MarshalPKCS8PrivateKey(control.server.TLS.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: control.server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateTLS}), 0600); err != nil {
		t.Fatal(err)
	}
	signer, jwks := grantKeys(t)
	jwksPath := filepath.Join(dir, "public-jwks.json")
	if err = os.WriteFile(jwksPath, jwks, 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	args := []string{"-search-listen", address, "-search-tls-cert", certPath, "-search-tls-key", keyPath, "-search-verification-keys", jwksPath}
	var children []*workerProcess
	if live {
		t.Cleanup(func() {
			names := map[string]bool{}
			for _, p := range children {
				p.mu.Lock()
				for name := range p.seen {
					names[name] = true
				}
				p.mu.Unlock()
			}
			if store, err := OpenStore(f.path); err == nil {
				if execution, err := store.Execution(); err == nil && execution.Owner.Name != "" {
					names[execution.Owner.Name] = true
				}
				store.Close()
			}
			for name := range names {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := target.Client.Collection(name).Delete(ctx)
				cancel()
				if err != nil {
					t.Errorf("grant target cleanup failed: %s", name)
				}
			}
		})
	}
	startChild := func() *workerProcess {
		p := startManagedSearchProcess(t, binary, f, control, key, certPath, args)
		children = append(children, p)
		return p
	}
	child := startChild()
	first := control.wait(t, child, func(o Observation) bool { return o.Phase == "active" })
	client := control.server.Client()
	client.Timeout = 3 * time.Second
	call := func(term, token string) (int, *SearchResponse) {
		t.Helper()
		raw, _ := json.Marshal(SearchRequest{Field: "title", Term: term})
		request, _ := http.NewRequest("POST", "https://"+address+"/v1/bindings/"+f.c.BindingID+"/search", bytes.NewReader(raw))
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(request)
		if err != nil {
			return 0, nil
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return response.StatusCode, nil
		}
		var result SearchResponse
		if json.NewDecoder(response.Body).Decode(&result) != nil {
			t.Fatal("bad signed search response")
		}
		return 200, &result
	}
	mint := func(revision uint64, lifetime time.Duration) string {
		return signGrant(t, signer, grantClaims(f.c, revision, lifetime))
	}
	await := func(term string, token string) *SearchResponse {
		t.Helper()
		end := time.After(2 * time.Minute)
		for {
			code, result := call(term, token)
			if code == 200 && len(result.Docs) == 1 {
				return result
			}
			select {
			case <-child.done:
				t.Fatalf("signed worker exited: %s", child.stderr.String())
			case <-end:
				t.Fatalf("signed search unavailable: %d", code)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	firstGrant := mint(1, time.Minute)
	initial := await("baseline", firstGrant)
	if initial.Epoch != first.Epoch {
		t.Fatal("wrong signed serving epoch")
	}
	if code, _ := call("baseline", "synthetic-read-only-search-at-least-32-bytes"); code != 401 {
		t.Fatal("static fallback accepted in signed mode", code)
	}
	if code, _ := call("baseline", control.token); code != 401 {
		t.Fatal("worker control credential can search", code)
	}
	wrong := grantClaims(f.c, 1, time.Minute)
	wrong.BindingID = "other"
	if code, _ := call("baseline", signGrant(t, signer, wrong)); code != 401 {
		t.Fatal("cross binding grant accepted", code)
	}
	t.Log("public-key-only worker accepted bounded signed grant and rejected legacy/control credentials")
	// No issuance occurs during the simulated central outage. The already issued
	// grant remains valid offline, then expires. Capture has independent authority.
	cached := mint(1, 3*time.Second)
	claims, _ := jwtExpiry(cached)
	control.unavailable(true)
	if code, _ := call("baseline", cached); code != 200 {
		t.Fatal("cached valid grant depended on central control", code)
	}
	timer := time.NewTimer(time.Until(claims) + 20*time.Millisecond)
	<-timer.C
	if code, _ := call("baseline", cached); code != 401 {
		t.Fatal("expired cached grant accepted", code)
	}
	execSQL(t, f.sql, "UPDATE "+f.c.Schema+".items SET title='cedar' WHERE id=1")
	expected := sourceRows(t, f)
	if live {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		err = target.Verify(ctx, first.Generation, expected)
		cancel()
		if err != nil {
			t.Fatal("capture stopped after grant expiry", err)
		}
	} else {
		end := time.Now().Add(5 * time.Second)
		equal := false
		for time.Now().Before(end) {
			mock.mu.Lock()
			docs := mock.docs[first.Generation]
			want := map[string]any{}
			for id, row := range expected {
				want[id] = document(id, row)
			}
			equal = equalJSON(docs, want)
			mock.mu.Unlock()
			if equal {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !equal {
			t.Fatal("capture stopped after grant expiry")
		}
	}
	if code, _ := call("cedar", cached); code != 401 {
		t.Fatal("capture progress revived expired app grant", code)
	}
	control.unavailable(false)
	current := control.wait(t, child, func(o Observation) bool {
		return o.Phase == "active" && o.DesiredRevision == 1 && o.Sequence > first.Sequence
	})
	if current.Epoch != first.Epoch {
		t.Fatal("search expiry restarted capture")
	}
	await("cedar", mint(1, time.Minute))
	t.Log("capture continued while central delivery was unavailable and the app grant expired")
	control.set(2, "paused", time.Now().Add(time.Hour))
	control.wait(t, child, func(o Observation) bool { return o.DesiredRevision == 2 && o.Phase == "paused" })
	if code, _ := call("cedar", mint(1, time.Minute)); code != 401 {
		t.Fatal("unexpired pre-pause grant accepted", code)
	}
	control.set(3, "running", time.Now().Add(time.Hour))
	resumed := control.wait(t, child, func(o Observation) bool { return o.DesiredRevision == 3 && o.Phase == "active" })
	if code, _ := call("cedar", mint(1, time.Minute)); code != 401 {
		t.Fatal("resume revived obsolete grant revision", code)
	}
	latest := await("cedar", mint(3, time.Minute))
	if latest.Epoch != resumed.Epoch {
		t.Fatal("signed grant routed to retired execution")
	}
	if !live {
		cached = mint(3, time.Minute)
		control.unavailable(true)
		killManaged(t, child)
		waitInactive(t, f)
		child = startChild()
		recovered := await("cedar", cached)
		if recovered.Epoch <= latest.Epoch {
			t.Fatal("signed restart reused epoch")
		}
		control.unavailable(false)
		t.Log("cached approval, public verification keys and valid grant recovered through SIGKILL without central calls")
	}
	child.stop(t)
	waitInactive(t, f)
	t.Log("signed search scope, offline validation, expiry and pause/resume revision fencing verified")
}
func jwtExpiry(raw string) (time.Time, error) {
	var claims SearchGrantClaims
	_, _, err := jwt.NewParser().ParseUnverified(raw, &claims)
	if err != nil || claims.ExpiresAt == nil {
		return time.Time{}, fmt.Errorf("invalid test grant")
	}
	return claims.ExpiresAt.Time, nil
}
