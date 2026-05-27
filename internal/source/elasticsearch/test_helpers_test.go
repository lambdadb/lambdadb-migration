package elasticsearch

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type request = *http.Request
type responseWriter = http.ResponseWriter
type handlerFunc func(*testing.T, responseWriter, request)

func newTestServer(t *testing.T, handlers map[string]handlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		if fn, ok := handlers[key]; ok {
			fn(t, w, r)
			return
		}
		t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
	}))
	t.Cleanup(server.Close)
	return server
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
