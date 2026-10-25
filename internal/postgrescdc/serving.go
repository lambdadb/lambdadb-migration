package postgrescdc

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
)

const MaxServingRows = 100
const servingHealthAge = 5 * time.Second

var servingTerm = regexp.MustCompile(`^[a-z0-9]{1,32}$`)
var servingText = regexp.MustCompile(`^[A-Za-z0-9 ]*$`)
var errSearchUnavailable = errors.New("regional search is unavailable")
var errSearchRequest = errors.New("unsupported regional search request")

type SearchRequest struct {
	Field string `json:"field"`
	Term  string `json:"term"`
	Limit int    `json:"limit,omitempty"`
}
type SearchResponse struct {
	BindingID    string           `json:"binding_id"`
	Epoch        uint64           `json:"epoch"`
	ValidatedLSN string           `json:"validated_lsn"`
	Mode         string           `json:"mode"`
	Docs         []map[string]any `json:"docs"`
}

// SearchTarget uses the supported data-plane query API. Full matches are read
// before applying the caller's limit so deleted/stale extra hits cannot hide.
type SearchTarget interface {
	SearchToken(context.Context, string, string, string) ([]map[string]any, int64, error)
}
type servingRun struct {
	generation Generation
	config     Config
	target     SearchTarget
	ctx        context.Context
	healthyAt  time.Time
	ready      bool
	serial     uint64
}

// ServingGate resolves only the current in-process execution. Nothing persisted
// on disk or received from central telemetry can make a restarted gate ready.
// Mutations invalidate queries instead of waiting for remote reads.
type ServingGate struct {
	slots   chan struct{}
	store   *Store
	mu      sync.Mutex
	current servingRun
}

func NewServingGate(store *Store) *ServingGate {
	return &ServingGate{store: store, slots: make(chan struct{}, 4)}
}
func (g *ServingGate) clear() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.current.ready = false
	g.current.serial++
}
func (g *ServingGate) start(ctx context.Context, run Generation, c Config, target SearchTarget) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	serial := g.current.serial + 1
	g.current = servingRun{generation: run, config: c, target: target, ctx: ctx, serial: serial}
}
func (g *ServingGate) healthy(run Generation) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current.generation.Epoch == run.Epoch {
		g.current.healthyAt = time.Now()
	}
}
func (g *ServingGate) ready(run Generation) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current.generation.Epoch == run.Epoch {
		g.current.ready = true
		g.current.serial++
	}
}
func (g *ServingGate) snapshot() servingRun { g.mu.Lock(); defer g.mu.Unlock(); return g.current }
func (g *ServingGate) valid(run servingRun) bool {
	current := g.snapshot()
	return current.ready && current.serial == run.serial && current.ctx != nil && current.ctx.Err() == nil && time.Since(current.healthyAt) < servingHealthAge && g.store.checkManaged(run.generation) == nil
}

// Finish under the same mutex used to announce mutations. This is the local
// response authorization point; network delivery is not an instant revocation.
func (g *ServingGate) finish(run servingRun, lsn uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	current := g.current
	if !current.ready || current.serial != run.serial || current.ctx == nil || current.ctx.Err() != nil || time.Since(current.healthyAt) >= servingHealthAge {
		return false
	}
	latest, err := g.store.Load()
	return err == nil && latest != nil && latest.LSN == lsn && g.store.checkManaged(run.generation) == nil
}
func searchExpected(c Config, state *State, request SearchRequest) (map[string]any, error) {
	var indexes map[string]map[string]json.RawMessage
	if json.Unmarshal(c.IndexConfigs, &indexes) != nil {
		return nil, errSearchRequest
	}
	config := indexes[request.Field]
	var kind string
	if !name.MatchString(request.Field) || len(config) != 1 || json.Unmarshal(config["type"], &kind) != nil || kind != "text" {
		return nil, errSearchRequest
	}
	if len(state.Rows) > MaxServingRows {
		return nil, errSearchUnavailable
	}
	expected := map[string]any{}
	for id, row := range state.Rows {
		raw, ok := row[request.Field]
		if !ok {
			return nil, errSearchUnavailable
		}
		var value string
		if json.Unmarshal(raw, &value) != nil || !servingText.MatchString(value) {
			return nil, errSearchUnavailable
		}
		for _, word := range strings.Fields(strings.ToLower(value)) {
			if word == request.Term {
				expected[id] = document(id, row)
				break
			}
		}
	}
	return expected, nil
}
func (g *ServingGate) Search(ctx context.Context, bindingID string, request SearchRequest) (*SearchResponse, error) {
	if !servingTerm.MatchString(request.Term) || request.Limit < 0 || request.Limit > MaxServingRows {
		return nil, errSearchRequest
	}
	if request.Limit == 0 {
		request.Limit = 10
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	select {
	case g.slots <- struct{}{}:
		defer func() { <-g.slots }()
	default:
		return nil, errSearchUnavailable
	}
	run := g.snapshot()
	if run.config.BindingID != bindingID || run.target == nil || !g.valid(run) {
		return nil, errSearchUnavailable
	}
	state, err := g.store.Load()
	if err != nil || state == nil {
		return nil, errSearchUnavailable
	}
	e, err := g.store.Execution()
	if err != nil || e.Active == nil || e.Active.Epoch != run.generation.Epoch || e.Active.Generation != run.generation.Name || state.Fingerprint != run.generation.Fingerprint {
		return nil, errSearchUnavailable
	}
	expected, err := searchExpected(run.config, state, request)
	if err != nil {
		return nil, err
	}
	docs, total, err := run.target.SearchToken(ctx, run.generation.Name, request.Field, request.Term)
	// Query total can describe returned hits, not all possible matches. A full
	// size-100 response cannot prove that another stale hit was not truncated.
	if err != nil || len(docs) >= MaxServingRows || total != int64(len(expected)) || len(docs) != len(expected) {
		return nil, errSearchUnavailable
	}
	actual := map[string]any{}
	for _, doc := range docs {
		id, ok := doc["id"].(string)
		if !ok || actual[id] != nil {
			return nil, errSearchUnavailable
		}
		actual[id] = doc
	}
	if !equalJSON(actual, expected) {
		return nil, errSearchUnavailable
	}
	// Check again after all remote reads, before releasing any source data. A
	// response racing pause, expiry, mutation, replacement or capture failure fails.
	if !g.finish(run, state.LSN) || ctx.Err() != nil {
		return nil, errSearchUnavailable
	}
	if len(docs) > request.Limit {
		docs = docs[:request.Limit]
	}
	return &SearchResponse{BindingID: bindingID, Epoch: run.generation.Epoch, ValidatedLSN: pglogrepl.LSN(state.LSN).String(), Mode: "query_string_single_token", Docs: docs}, nil
}

// Handler retains the explicit static-token local spike mode. A configured
// signed handler never falls back to this credential.
func (g *ServingGate) Handler(bindingID, token string) (http.Handler, error) {
	if !controlID.MatchString(bindingID) || len(token) < 32 {
		return nil, errors.New("binding identity and separate search credential required")
	}
	return g.handler(bindingID, func(supplied string) error {
		if len(supplied) != len(token) || subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) != 1 {
			return errors.New("invalid search credential")
		}
		return nil
	}), nil
}
func (g *ServingGate) SignedHandler(bindingID string, verifier *SearchGrantVerifier) (http.Handler, error) {
	if !controlID.MatchString(bindingID) || verifier == nil || len(verifier.keys) == 0 {
		return nil, errors.New("binding identity and search verification keys required")
	}
	return g.handler(bindingID, func(raw string) error { return verifier.authorize(raw, bindingID, g.store) }), nil
}
func (g *ServingGate) handler(bindingID string, authorize func(string) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		secret := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || authorize(secret) != nil {
			http.Error(w, `{"error":"unauthorized"}`, 401)
			return
		}
		if r.Method != "POST" || r.URL.Path != "/v1/bindings/"+bindingID+"/search" || r.URL.RawQuery != "" {
			http.NotFound(w, r)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		var request SearchRequest
		var extra any
		if d.Decode(&request) != nil || d.Decode(&extra) != io.EOF {
			http.Error(w, `{"error":"invalid search request"}`, 400)
			return
		}
		if authorize(secret) != nil {
			http.Error(w, `{"error":"search grant no longer valid"}`, 401)
			return
		}
		response, err := g.Search(r.Context(), bindingID, request)
		if errors.Is(err, errSearchRequest) {
			http.Error(w, `{"error":"unsupported search request"}`, 400)
			return
		}
		if err != nil {
			http.Error(w, `{"error":"regional search unavailable"}`, 503)
			return
		}
		if authorize(secret) != nil {
			http.Error(w, `{"error":"search grant no longer valid"}`, 401)
			return
		}
		_ = json.NewEncoder(w).Encode(response)
	})
}
