package postgrescdc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type searchFunc func(context.Context, string, string, string) ([]map[string]any, int64, error)

func (f searchFunc) SearchToken(ctx context.Context, generation, field, term string) ([]map[string]any, int64, error) {
	return f(ctx, generation, field, term)
}
func servingFixture(t *testing.T) (*ServingGate, Generation, *State, Config) {
	return servingFixtureFor(t, testConfig())
}
func servingFixtureFor(t *testing.T, c Config) (*ServingGate, Generation, *State, Config) {
	t.Helper()
	s := openTestStore(t, filepath.Join(t.TempDir(), "serving.db"))
	if err := s.AcceptControl(approvedTest(c), c.BindingID); err != nil {
		t.Fatal(err)
	}
	run, err := s.begin(c, true)
	if err != nil {
		t.Fatal(err)
	}
	run, err = s.nameGeneration(run, "generation_one")
	if err != nil {
		t.Fatal(err)
	}
	state := decoderState()
	state.Fingerprint = c.fingerprint()
	state.Columns = []Column{{Name: "id", OID: 20, Key: true, Modifier: -1}, {Name: "title", OID: 25, Modifier: -1}}
	state.Rows = map[string]Row{"1": {"id": json.RawMessage(`1`), "title": json.RawMessage(`"cedar cedar"`)}, "2": {"id": json.RawMessage(`2`), "title": json.RawMessage(`"pine"`)}}
	if err = s.save(state, &run); err != nil {
		t.Fatal(err)
	}
	digest, _ := rowsDigest(state.Rows)
	if err = s.recordVerified(run, state.LSN, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = s.activate(run, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g := NewServingGate(s)
	target := searchFunc(func(context.Context, string, string, string) ([]map[string]any, int64, error) {
		return []map[string]any{document("1", state.Rows["1"])}, 1, nil
	})
	g.start(ctx, run, c, target)
	g.healthy(run)
	g.ready(run)
	return g, run, state, c
}
func TestServingRequiresExactCurrentQueryResults(t *testing.T) {
	g, run, state, c := servingFixture(t)
	request := SearchRequest{Field: "title", Term: "cedar", Limit: 1}
	result, err := g.Search(context.Background(), c.BindingID, request)
	if err != nil || len(result.Docs) != 1 || result.ValidatedLSN != "0/64" || result.Epoch != run.Epoch {
		t.Fatal("current query rejected", err)
	}
	for _, tc := range []struct {
		name  string
		docs  []map[string]any
		total int64
	}{
		{"missing", nil, 0}, {"stale extra", []map[string]any{document("1", state.Rows["1"]), document("2", state.Rows["2"])}, 2},
		{"wrong fields", []map[string]any{{"id": "1", "title": "old"}}, 1},
		{"duplicates", []map[string]any{document("1", state.Rows["1"]), document("1", state.Rows["1"])}, 1},
		{"inaccurate total", []map[string]any{document("1", state.Rows["1"])}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g.mu.Lock()
			g.current.target = searchFunc(func(context.Context, string, string, string) ([]map[string]any, int64, error) {
				return tc.docs, tc.total, nil
			})
			g.mu.Unlock()
			if response, err := g.Search(context.Background(), c.BindingID, request); err == nil || response != nil {
				t.Fatal("unverified results exposed")
			}
		})
	}
}
func TestServingFailsClosedAcrossTransitions(t *testing.T) {
	for _, transition := range []string{"pause", "expire", "replacement", "capture failure", "stale health", "in flight mutation", "in flight pause"} {
		t.Run(transition, func(t *testing.T) {
			g, run, state, c := servingFixture(t)
			request := SearchRequest{Field: "title", Term: "cedar"}
			called := false
			inflight := func() {}
			pause := func() {
				a := approvedTest(c)
				a.Control.DesiredRevision = 2
				a.Control.DesiredState = "paused"
				if err := g.store.AcceptControl(a, c.BindingID); err != nil {
					t.Fatal(err)
				}
			}
			switch transition {
			case "pause":
				pause()
			case "expire":
				a := approvedTest(c)
				a.Control.DesiredRevision = 2
				a.Control.AuthorityExpiresAt = time.Now().Add(-time.Second)
				if err := g.store.AcceptControl(a, c.BindingID); err != nil {
					t.Fatal(err)
				}
			case "replacement":
				if _, err := g.store.begin(c, true); err != nil {
					t.Fatal(err)
				}
			case "capture failure":
				g.clear()
			case "stale health":
				g.mu.Lock()
				g.current.healthyAt = time.Now().Add(-servingHealthAge)
				g.mu.Unlock()
			case "in flight mutation":
				inflight = func() { g.clear(); g.ready(run) }
			case "in flight pause":
				inflight = pause
			}
			g.mu.Lock()
			g.current.target = searchFunc(func(context.Context, string, string, string) ([]map[string]any, int64, error) {
				called = true
				inflight()
				return []map[string]any{document("1", state.Rows["1"])}, 1, nil
			})
			g.mu.Unlock()
			if response, err := g.Search(context.Background(), c.BindingID, request); err == nil || response != nil {
				t.Fatal("transition exposed stale results")
			}
			if strings.HasPrefix(transition, "in flight") && !called {
				t.Fatal("in-flight query path was not exercised")
			}
		})
	}
}
func TestServingHTTPBoundary(t *testing.T) {
	g, _, _, c := servingFixture(t)
	token := strings.Repeat("search-only-", 3)
	handler, err := g.Handler(c.BindingID, token)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body, token, path string
		want              int
	}{
		{`{"field":"title","term":"cedar"}`, token, "/v1/bindings/test/search", 200},
		{`{"field":"title","term":"cedar"}`, "worker-token", "/v1/bindings/test/search", 401},
		{`{"field":"title","term":"cedar","ref":"old"}`, token, "/v1/bindings/test/search", 400},
		{`{"field":"title","term":"cedar"} {}`, token, "/v1/bindings/test/search", 400},
		{`{"field":"title","term":"cedar OR pine"}`, token, "/v1/bindings/test/search", 400},
		{`{"field":"body","term":"cedar"}`, token, "/v1/bindings/test/search", 400},
		{`{"field":"title","term":"cedar"}`, token, "/v1/bindings/other/search", 404},
	} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s: got %d want %d", tc.body, w.Code, tc.want)
		}
		if strings.Contains(w.Body.String(), "generation_one") || strings.Contains(w.Body.String(), "TEST_KEY") {
			t.Fatal("target identity leaked")
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("search response cacheable")
		}
	}
}

func TestServingBudgetsAndSourcePredicate(t *testing.T) {
	g, run, state, c := servingFixture(t)
	request := SearchRequest{Field: "title", Term: "cedar"}
	for i := 0; i < cap(g.slots); i++ {
		g.slots <- struct{}{}
	}
	if result, err := g.Search(context.Background(), c.BindingID, request); err == nil || result != nil {
		t.Fatal("concurrency budget ignored")
	}
	for i := 0; i < cap(g.slots); i++ {
		<-g.slots
	}
	state.Rows["2"]["title"] = json.RawMessage(`"unicode café"`)
	if err := g.store.save(state, &run); err != nil {
		t.Fatal(err)
	}
	if result, err := g.Search(context.Background(), c.BindingID, request); err == nil || result != nil {
		t.Fatal("unsupported source analyzer predicate served")
	}
	state.Rows = map[string]Row{}
	for i := 0; i < MaxServingRows+1; i++ {
		state.Rows[fmt.Sprint(i)] = Row{}
	}
	if _, err := searchExpected(c, state, request); err == nil {
		t.Fatal("row budget ignored")
	}
}

func TestServingRejectsPossiblyTruncatedResults(t *testing.T) {
	g, run, state, c := servingFixture(t)
	g.clear()
	state.LSN++
	state.Rows = map[string]Row{}
	docs := []map[string]any{}
	for i := 1; i <= MaxServingRows; i++ {
		id := fmt.Sprint(i)
		row := Row{"id": json.RawMessage(id), "title": json.RawMessage(`"cedar"`)}
		state.Rows[id] = row
		docs = append(docs, document(id, row))
	}
	if err := g.store.save(state, &run); err != nil {
		t.Fatal(err)
	}
	g.ready(run)
	g.mu.Lock()
	g.current.target = searchFunc(func(context.Context, string, string, string) ([]map[string]any, int64, error) {
		return docs, int64(len(docs)), nil
	})
	g.mu.Unlock()
	if result, err := g.Search(context.Background(), c.BindingID, SearchRequest{Field: "title", Term: "cedar", Limit: 100}); err == nil || result != nil {
		t.Fatal("top-100 truncation treated as complete match proof")
	}
}
