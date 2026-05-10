package integration_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	migrationcmd "github.com/lambdadb/lambdadb-migration/cmd"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	qdrantapi "github.com/qdrant/go-client/qdrant"
)

func TestQdrantToLambdaDBMockIntegration(t *testing.T) {
	if os.Getenv("LAMBDADB_MIGRATION_RUN_INTEGRATION") != "1" {
		t.Skip("set LAMBDADB_MIGRATION_RUN_INTEGRATION=1 and run Qdrant from integration_tests/compose/qdrant.yaml")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	qdrantURL := os.Getenv("LAMBDADB_MIGRATION_QDRANT_URL")
	if qdrantURL == "" {
		qdrantURL = "http://localhost:6334"
	}
	collection := fmt.Sprintf("lambdadb_migration_it_%d", time.Now().UnixNano())
	seedQdrantCollection(t, ctx, qdrantURL, collection)

	mock := newLambdaDBMock(t, "playground", "articles")
	defer mock.server.Close()

	cmd := migrationcmd.MigrateQdrantCmd{
		Qdrant: config.QdrantConfig{
			URL:            qdrantURL,
			Collection:     collection,
			MaxMessageSize: 32 * 1024 * 1024,
		},
		LambdaDB: config.LambdaDBConfig{
			BaseURL:     mock.server.URL,
			ProjectName: "playground",
			APIKey:      "test-key",
			Collection:  "articles",
		},
		Migration: config.MigrationConfig{
			BatchSize:        2,
			MaxBatchBytes:    6_000_000,
			WriteMode:        config.WriteModeUpsert,
			Restart:          true,
			CreateCollection: true,
			CheckpointPath:   t.TempDir(),
		},
	}
	if err := cmd.Run(&migrationcmd.Globals{}); err != nil {
		t.Fatalf("migration Run() error = %v", err)
	}

	docs := mock.docs()
	if got, want := len(docs), 2; got != want {
		t.Fatalf("mock accepted %d docs, want %d: %#v", got, want, docs)
	}
	byID := map[string]map[string]any{}
	for _, doc := range docs {
		id, _ := doc["id"].(string)
		byID[id] = doc
	}
	doc := byID["1"]
	if doc == nil || byID["2"] == nil {
		t.Fatalf("docs by ID = %#v, want IDs 1 and 2", byID)
	}
	if _, ok := doc["dense"]; !ok {
		t.Fatalf("doc = %#v, want dense vector field", doc)
	}
	if doc["metadata_url"] != "https://example.com/1" {
		t.Fatalf("doc = %#v, want normalized metadata_url", doc)
	}
}

func seedQdrantCollection(t *testing.T, ctx context.Context, rawURL, collection string) {
	t.Helper()

	client, err := newQdrantClient(rawURL)
	if err != nil {
		t.Fatalf("create qdrant client: %v", err)
	}
	defer client.Close()

	if err := client.DeleteCollection(ctx, collection); err != nil && !isNotFound(err) {
		t.Fatalf("delete existing qdrant collection: %v", err)
	}
	if err := client.CreateCollection(ctx, &qdrantapi.CreateCollection{
		CollectionName: collection,
		VectorsConfig: qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
			Size:     3,
			Distance: qdrantapi.Distance_Cosine,
		}),
	}); err != nil {
		t.Fatalf("create qdrant collection: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupClient, err := newQdrantClient(rawURL)
		if err != nil {
			t.Logf("create qdrant cleanup client: %v", err)
			return
		}
		defer cleanupClient.Close()
		if err := cleanupClient.DeleteCollection(cleanupCtx, collection); err != nil && !isNotFound(err) {
			t.Logf("delete qdrant collection %q: %v", collection, err)
		}
	})

	points := []*qdrantapi.PointStruct{
		{
			Id:      qdrantapi.NewIDNum(1),
			Vectors: qdrantapi.NewVectors(0.1, 0.2, 0.3),
			Payload: qdrantapi.NewValueMap(map[string]any{
				"title":        "one",
				"metadata.url": "https://example.com/1",
			}),
		},
		{
			Id:      qdrantapi.NewIDNum(2),
			Vectors: qdrantapi.NewVectors(0.4, 0.5, 0.6),
			Payload: qdrantapi.NewValueMap(map[string]any{
				"title":        "two",
				"metadata.url": "https://example.com/2",
			}),
		},
	}
	if _, err := client.Upsert(ctx, &qdrantapi.UpsertPoints{
		CollectionName: collection,
		Wait:           qdrantapi.PtrOf(true),
		Points:         points,
	}); err != nil {
		t.Fatalf("seed qdrant points: %v", err)
	}
}

func newQdrantClient(rawURL string) (*qdrantapi.Client, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	port := 6334
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil {
			return nil, err
		}
	}
	return qdrantapi.NewClient(&qdrantapi.Config{
		Host:                   parsed.Hostname(),
		Port:                   port,
		UseTLS:                 parsed.Scheme == "https",
		SkipCompatibilityCheck: true,
	})
}

func isNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

type lambdaDBMock struct {
	t          *testing.T
	project    string
	collection string
	server     *httptest.Server
	mu         sync.Mutex
	accepted   []map[string]any
}

func newLambdaDBMock(t *testing.T, project, collection string) *lambdaDBMock {
	t.Helper()

	mock := &lambdaDBMock{t: t, project: project, collection: collection}
	mock.server = httptest.NewServer(http.HandlerFunc(mock.handle))
	return mock
}

func (m *lambdaDBMock) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	expectedBase := path.Join("/projects", m.project, "collections")
	collectionPath := path.Join(expectedBase, m.collection)

	switch {
	case r.Method == http.MethodGet && r.URL.Path == collectionPath:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"not found"}`))
	case r.Method == http.MethodPost && r.URL.Path == expectedBase:
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"collection":{"collectionName":"` + m.collection + `","numDocs":0,"createdAt":1700000000,"updatedAt":1700000000,"dataUpdatedAt":1700000000}}`))
	case r.Method == http.MethodPost && r.URL.Path == path.Join(collectionPath, "docs", "upsert"):
		var body struct {
			Docs []map[string]any `json:"docs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.accepted = append(m.accepted, body.Docs...)
		m.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"message":"accepted"}`))
	default:
		m.t.Errorf("unexpected LambdaDB mock request: %s %s", r.Method, r.URL.Path)
		http.NotFound(w, r)
	}
}

func (m *lambdaDBMock) docs() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]map[string]any, len(m.accepted))
	copy(out, m.accepted)
	return out
}
