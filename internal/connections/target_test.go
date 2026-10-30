package connections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
func TestOrdinaryAPILostResponseNeverReplaysAfterRestart(t *testing.T) {
	requests := 0
	target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/projects/customer-project/collections/items/docs/upsert" || r.Header.Get("x-api-key") != "synthetic-project-key" || r.Header.Get("x-lambdadb-version") != "v3" || r.Header.Get("x-lambdadb-project-id") != "native-project-id" {
			t.Error("incorrect target routing")
		}
		var body map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		if len(body) != 1 || body["docs"] == nil || !strings.Contains(string(body["docs"]), "9223372036854775807") {
			t.Error("non-public or changed request content")
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close() // The target may already have applied the request.
	})
	path := filepath.Join(t.TempDir(), "state.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.Apply(context.Background(), target, "upsert", json.RawMessage(`{"docs":[{"id":"one","n":9223372036854775807}]}`)); !errors.Is(err, ErrTargetOutcomeUncertain) {
		t.Fatal(err)
	}
	_ = j.Close()
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err = j.RecoverPending(context.Background(), target); !errors.Is(err, ErrTargetOutcomeUncertain) {
		t.Fatal(err)
	}
	if err = j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`)); !errors.Is(err, ErrPending) {
		t.Fatal(err)
	}
	state, _ := j.Load()
	if requests != 1 || state.AcceptedSequence != 0 || state.Pending.Status != "sending" || len(state.ManagedIDs) != 0 {
		t.Fatal("uncertain request replayed or checkpoint advanced")
	}
}

func TestOrdinaryAcceptanceAndDefiniteRejectionClassification(t *testing.T) {
	for _, test := range []struct {
		status int
		body   string
		want   error
	}{
		{202, `{"message":"Upsert request is accepted"}`, nil},
		{202, `{"ingestion":{"connectionId":"connection","sequence":1}}`, ErrTargetOutcomeUncertain},
		{202, strings.Repeat("x", 4097), ErrTargetOutcomeUncertain},
		{400, `{"message":"invalid"}`, ErrTargetRejected},
		{401, `{"message":"unauthorized"}`, ErrTargetRejected},
		{408, `{}`, ErrTargetOutcomeUncertain},
		{503, `{}`, ErrTargetOutcomeUncertain},
	} {
		t.Run(fmt.Sprint(test.status, len(test.body)), func(t *testing.T) {
			calls := 0
			target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			})
			j, err := OpenJournal(filepath.Join(t.TempDir(), "state.db"), "binding", true)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			err = j.Apply(context.Background(), target, "delete", json.RawMessage(`{"ids":["one"]}`))
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
			state, _ := j.Load()
			if test.want == nil {
				if state.Pending != nil || state.AcceptedSequence != 1 {
					t.Fatal("acceptance not persisted")
				}
			} else {
				if state.Pending == nil || state.AcceptedSequence != 0 {
					t.Fatal("failed request advanced checkpoint")
				}
				if !errors.Is(j.RecoverPending(context.Background(), target), test.want) {
					t.Fatal("failure classification lost")
				}
			}
			if calls != 1 {
				t.Fatal("request automatically retried")
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
		{`{"collection":{"projectName":"customer-project","collectionName":"items"}}`, true},
		{`{"projectName":"customer-project","collectionName":"items"}`, false},
		{`{"collection":{"projectName":"another-project","collectionName":"items"}}`, false},
	} {
		target, _ := targetFixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(test.body)) })
		meta, err := target.Describe(context.Background())
		if (err == nil) != test.valid || (test.valid && (meta == nil || meta.CollectionName != "items")) {
			t.Fatal("public metadata boundary mismatch", err)
		}
	}
}

type intentSender func(context.Context, Intent) error

func (f intentSender) Send(ctx context.Context, p Intent) error { return f(ctx, p) }

func TestSuccessfulResponseWithFailedAcceptanceSaveRemainsUncertain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	target := intentSender(func(context.Context, Intent) error { calls++; return j.Close() })
	err = j.Apply(context.Background(), target, "upsert", json.RawMessage(`{"docs":[{"id":"one"}]}`))
	if !errors.Is(err, ErrTargetOutcomeUncertain) {
		t.Fatal(err)
	}
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if !errors.Is(j.RecoverPending(context.Background(), target), ErrTargetOutcomeUncertain) || calls != 1 {
		t.Fatal("successful but unrecorded request replayed")
	}
	state, _ := j.Load()
	if state.AcceptedSequence != 0 || len(state.ManagedIDs) != 0 {
		t.Fatal("unrecorded acceptance advanced state")
	}
}

func TestPreparedUnsentIntentCanContinueAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	j, err := OpenJournal(path, "binding", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.Prepare("delete", json.RawMessage(`{"ids":["one"]}`)); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	j, err = OpenJournal(path, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	calls := 0
	if err = j.RecoverPending(context.Background(), intentSender(func(context.Context, Intent) error { calls++; return nil })); err != nil {
		t.Fatal(err)
	}
	state, _ := j.Load()
	if calls != 1 || state.Pending != nil || state.AcceptedSequence != 1 {
		t.Fatal("unsent intent did not progress")
	}
}
