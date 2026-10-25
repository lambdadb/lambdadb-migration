package connections

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/connections/protocol"
)

func targetFixture(t *testing.T, handler http.HandlerFunc) (*APITarget, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	target, err := NewAPITarget(protocol.Binding{ConnectionID: "connection", Target: protocol.Target{URL: server.URL, Project: "customer-project", Collection: "items", Version: 3, SharedGateway: true, ProjectID: "native-project-id"}}, "synthetic-project-key")
	if err != nil {
		t.Fatal(err)
	}
	target.client.Transport = server.Client().Transport
	return target, server
}
func TestOrderedAPIResponseLossAndRestartRetainTheSameIntent(t *testing.T) {
	// This fixture implements the documented wire contract. Durable engine behavior
	// is separately tested in EfsBranchWalTest and WriterMutationCommitterTest.
	var mu sync.Mutex
	var last uint64
	var document json.RawMessage
	var requests int
	target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requests++
		if r.Method != "POST" || !strings.HasPrefix(r.URL.Path, "/projects/customer-project/collections/items/docs/") || r.Header.Get("x-api-key") != "synthetic-project-key" || r.Header.Get("x-lambdadb-version") != "v3" || r.Header.Get("x-lambdadb-project-id") != "native-project-id" {
			t.Error("incorrect fixed target routing or authentication")
			w.WriteHeader(400)
			return
		}
		var body struct {
			Docs      json.RawMessage `json:"docs"`
			IDs       []string        `json:"ids"`
			Ingestion struct {
				ConnectionID string `json:"connectionId"`
				Sequence     uint64 `json:"sequence"`
			} `json:"ingestion"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			w.WriteHeader(400)
			return
		}
		n := body.Ingestion.Sequence
		if n > last+1 {
			w.WriteHeader(409)
			return
		}
		if n > last {
			last = n
			if len(body.IDs) > 0 {
				document = nil
			} else {
				document = body.Docs
			}
		}
		if requests == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": "accepted", "ingestion": body.Ingestion})
	})
	path := filepath.Join(t.TempDir(), "state.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	if j.Apply(context.Background(), target, "upsert", json.RawMessage(`{"docs":[{"id":"one","n":9223372036854775807,"title":"first"}]}`)) == nil {
		t.Fatal("lost response acknowledged")
	}
	state, _ := j.Load()
	old := *state.Pending
	if state.AcceptedSequence != 0 || old.Sequence != 1 {
		t.Fatal("uncertain operation discarded")
	}
	_ = j.Close()
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.RecoverPending(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if err = j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`)); err != nil {
		t.Fatal(err)
	}
	if err = j.Apply(context.Background(), target, "upsert", json.RawMessage(`{"docs":[{"id":"one","title":"newest"}]}`)); err != nil {
		t.Fatal(err)
	}
	if err = target.Send(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if last != 3 || string(document) != `[{"id":"one","title":"newest"}]` || requests != 5 {
		t.Fatal("fixed target retry contract regressed or required readback", last, string(document), requests)
	}
	state, _ = j.Load()
	if state.Pending != nil || state.AcceptedSequence != 3 {
		t.Fatal("checkpoint did not follow exact acceptance")
	}
}
func TestGeneric202AndMismatchedAcceptanceNeverClearPending(t *testing.T) {
	for _, reply := range []string{`{"message":"accepted"}`, `{"ingestion":{"connectionId":"other","sequence":1}}`, `{"ingestion":{"connectionId":"connection","sequence":2}}`, strings.Repeat("x", 4097)} {
		t.Run(fmt.Sprint(len(reply), reply[:8]), func(t *testing.T) {
			target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202); _, _ = w.Write([]byte(reply)) })
			j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			if j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`)) == nil {
				t.Fatal("incompatible target acknowledged")
			}
			state, _ := j.Load()
			if state.Pending == nil || state.AcceptedSequence != 0 {
				t.Fatal("unproven acceptance advanced state")
			}
		})
	}
}
func TestTargetRedirectDoesNotForwardCredentialsOrAcknowledge(t *testing.T) {
	visited := false
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { visited = true }))
	defer destination.Close()
	target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, 307) })
	j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`)) == nil || visited {
		t.Fatal("redirect followed or accepted")
	}
}

func TestDescribeRequiresPublicCollectionResponseEnvelope(t *testing.T) {
	for _, test := range []struct {
		body  string
		valid bool
	}{
		{`{"collection":{"projectName":"customer-project","collectionName":"items","connectionId":"connection"}}`, true},
		{`{"projectName":"customer-project","collectionName":"items","connectionId":"connection"}`, false},
		{`{"collection":{"projectName":"another-project","collectionName":"items","connectionId":"connection"}}`, false},
	} {
		target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(test.body)) })
		meta, err := target.Describe(context.Background())
		if (err == nil) != test.valid || (test.valid && (meta == nil || meta.ConnectionID != "connection")) {
			t.Fatal("public metadata boundary mismatch", err)
		}
	}
}
