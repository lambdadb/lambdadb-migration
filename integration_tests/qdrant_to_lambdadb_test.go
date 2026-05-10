package integration_tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

	tests := []struct {
		name       string
		fixture    qdrantFixture
		assertDocs func(*testing.T, []map[string]any)
		assertMock func(*testing.T, *lambdaDBMock)
		wantErr    string
	}{
		{
			name:    "unnamed_dense_with_dotted_payload",
			fixture: unnamedDenseFixture(),
			assertDocs: func(t *testing.T, docs []map[string]any) {
				t.Helper()
				requireDocCount(t, docs, 2)
				doc := requireDoc(t, docs, "1")
				requireField(t, doc, "dense")
				if doc["metadata_url"] != "https://example.com/1" {
					t.Fatalf("doc = %#v, want normalized metadata_url", doc)
				}
			},
		},
		{
			name:    "named_dense_vectors",
			fixture: namedDenseFixture(),
			assertDocs: func(t *testing.T, docs []map[string]any) {
				t.Helper()
				requireDocCount(t, docs, 2)
				doc := requireDoc(t, docs, "101")
				requireField(t, doc, "title_dense")
				requireField(t, doc, "body_dense")
				if _, ok := doc["dense"]; ok {
					t.Fatalf("doc = %#v, did not expect default dense field for named vectors", doc)
				}
			},
			assertMock: func(t *testing.T, mock *lambdaDBMock) {
				t.Helper()
				requireCreatedIndexFields(t, mock, "title_dense", "body_dense")
			},
		},
		{
			name:    "dense_sparse_and_payload_indexes",
			fixture: denseSparsePayloadIndexFixture(),
			assertDocs: func(t *testing.T, docs []map[string]any) {
				t.Helper()
				requireDocCount(t, docs, 2)
				doc := requireDoc(t, docs, "201")
				requireField(t, doc, "body_dense")
				sparse, ok := doc["keywords_sparse"].(map[string]any)
				if !ok {
					t.Fatalf("doc = %#v, want keywords_sparse object", doc)
				}
				if _, ok := sparse["3"]; !ok {
					t.Fatalf("keywords_sparse = %#v, want stringified sparse index 3", sparse)
				}
				if doc["category"] != "docs" || doc["views"] == nil {
					t.Fatalf("doc = %#v, want indexed payload fields", doc)
				}
			},
			assertMock: func(t *testing.T, mock *lambdaDBMock) {
				t.Helper()
				requireCreatedIndexFields(t, mock, "body_dense", "keywords_sparse", "category", "views")
			},
		},
		{
			name:    "manhattan_distance_rejected",
			fixture: manhattanFixture(),
			wantErr: `unsupported similarity "unsupported:manhattan"`,
		},
		{
			name:    "multi_vector_rejected",
			fixture: multiVectorFixture(),
			wantErr: "is a multi-vector and requires custom migration handling",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection := fmt.Sprintf("lambdadb_migration_it_%s_%d", tt.name, time.Now().UnixNano())
			seedQdrantCollection(t, ctx, qdrantURL, collection, tt.fixture)

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
					Validate:         tt.wantErr == "",
					CheckpointPath:   t.TempDir(),
				},
			}
			err := cmd.Run(&migrationcmd.Globals{})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("migration Run() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("migration Run() error = %v", err)
			}
			if tt.assertDocs != nil {
				tt.assertDocs(t, mock.docs())
			}
			if tt.assertMock != nil {
				tt.assertMock(t, mock)
			}
		})
	}
}

type qdrantFixture struct {
	vectorsConfig       *qdrantapi.VectorsConfig
	sparseVectorsConfig *qdrantapi.SparseVectorConfig
	payloadIndexes      []qdrantPayloadIndex
	points              []*qdrantapi.PointStruct
}

type qdrantPayloadIndex struct {
	field string
	typ   qdrantapi.FieldType
}

func unnamedDenseFixture() qdrantFixture {
	return qdrantFixture{
		vectorsConfig: qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
			Size:     3,
			Distance: qdrantapi.Distance_Cosine,
		}),
		points: []*qdrantapi.PointStruct{
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
		},
	}
}

func namedDenseFixture() qdrantFixture {
	return qdrantFixture{
		vectorsConfig: qdrantapi.NewVectorsConfigMap(map[string]*qdrantapi.VectorParams{
			"title_dense": {Size: 2, Distance: qdrantapi.Distance_Cosine},
			"body_dense":  {Size: 4, Distance: qdrantapi.Distance_Dot},
		}),
		points: []*qdrantapi.PointStruct{
			{
				Id: qdrantapi.NewIDNum(101),
				Vectors: qdrantapi.NewVectorsMap(map[string]*qdrantapi.Vector{
					"title_dense": qdrantapi.NewVectorDense([]float32{0.1, 0.2}),
					"body_dense":  qdrantapi.NewVectorDense([]float32{0.1, 0.2, 0.3, 0.4}),
				}),
				Payload: qdrantapi.NewValueMap(map[string]any{"title": "named one"}),
			},
			{
				Id: qdrantapi.NewIDNum(102),
				Vectors: qdrantapi.NewVectorsMap(map[string]*qdrantapi.Vector{
					"title_dense": qdrantapi.NewVectorDense([]float32{0.3, 0.4}),
					"body_dense":  qdrantapi.NewVectorDense([]float32{0.5, 0.6, 0.7, 0.8}),
				}),
				Payload: qdrantapi.NewValueMap(map[string]any{"title": "named two"}),
			},
		},
	}
}

func denseSparsePayloadIndexFixture() qdrantFixture {
	return qdrantFixture{
		vectorsConfig: qdrantapi.NewVectorsConfigMap(map[string]*qdrantapi.VectorParams{
			"body_dense": {Size: 3, Distance: qdrantapi.Distance_Cosine},
		}),
		sparseVectorsConfig: qdrantapi.NewSparseVectorsConfig(map[string]*qdrantapi.SparseVectorParams{
			"keywords_sparse": {},
		}),
		payloadIndexes: []qdrantPayloadIndex{
			{field: "category", typ: qdrantapi.FieldType_FieldTypeKeyword},
			{field: "views", typ: qdrantapi.FieldType_FieldTypeInteger},
		},
		points: []*qdrantapi.PointStruct{
			{
				Id: qdrantapi.NewIDNum(201),
				Vectors: qdrantapi.NewVectorsMap(map[string]*qdrantapi.Vector{
					"body_dense":      qdrantapi.NewVectorDense([]float32{0.2, 0.3, 0.4}),
					"keywords_sparse": qdrantapi.NewVectorSparse([]uint32{3, 9}, []float32{0.7, 0.2}),
				}),
				Payload: qdrantapi.NewValueMap(map[string]any{
					"category": "docs",
					"views":    int64(12),
				}),
			},
			{
				Id: qdrantapi.NewIDNum(202),
				Vectors: qdrantapi.NewVectorsMap(map[string]*qdrantapi.Vector{
					"body_dense":      qdrantapi.NewVectorDense([]float32{0.5, 0.6, 0.7}),
					"keywords_sparse": qdrantapi.NewVectorSparse([]uint32{1, 8}, []float32{0.4, 0.9}),
				}),
				Payload: qdrantapi.NewValueMap(map[string]any{
					"category": "guides",
					"views":    int64(34),
				}),
			},
		},
	}
}

func manhattanFixture() qdrantFixture {
	return qdrantFixture{
		vectorsConfig: qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
			Size:     2,
			Distance: qdrantapi.Distance_Manhattan,
		}),
		points: []*qdrantapi.PointStruct{
			{
				Id:      qdrantapi.NewIDNum(301),
				Vectors: qdrantapi.NewVectors(0.1, 0.2),
				Payload: qdrantapi.NewValueMap(map[string]any{"title": "manhattan"}),
			},
		},
	}
}

func multiVectorFixture() qdrantFixture {
	return qdrantFixture{
		vectorsConfig: qdrantapi.NewVectorsConfig(&qdrantapi.VectorParams{
			Size:              2,
			Distance:          qdrantapi.Distance_Cosine,
			MultivectorConfig: &qdrantapi.MultiVectorConfig{Comparator: qdrantapi.MultiVectorComparator_MaxSim},
		}),
		points: []*qdrantapi.PointStruct{
			{
				Id:      qdrantapi.NewIDNum(401),
				Vectors: qdrantapi.NewVectorsMulti([][]float32{{0.1, 0.2}, {0.3, 0.4}}),
				Payload: qdrantapi.NewValueMap(map[string]any{"title": "multi"}),
			},
		},
	}
}

func seedQdrantCollection(t *testing.T, ctx context.Context, rawURL, collection string, fixture qdrantFixture) {
	t.Helper()

	client, err := newQdrantClient(rawURL)
	if err != nil {
		t.Fatalf("create qdrant client: %v", err)
	}
	defer client.Close()

	_ = client.DeleteCollection(ctx, collection)
	if err := client.CreateCollection(ctx, &qdrantapi.CreateCollection{
		CollectionName:      collection,
		VectorsConfig:       fixture.vectorsConfig,
		SparseVectorsConfig: fixture.sparseVectorsConfig,
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

	if _, err := client.Upsert(ctx, &qdrantapi.UpsertPoints{
		CollectionName: collection,
		Wait:           qdrantapi.PtrOf(true),
		Points:         fixture.points,
	}); err != nil {
		t.Fatalf("seed qdrant points: %v", err)
	}
	for _, index := range fixture.payloadIndexes {
		fieldType := index.typ
		if _, err := client.CreateFieldIndex(ctx, &qdrantapi.CreateFieldIndexCollection{
			CollectionName: collection,
			Wait:           qdrantapi.PtrOf(true),
			FieldName:      index.field,
			FieldType:      &fieldType,
		}); err != nil {
			t.Fatalf("create qdrant payload index %q: %v", index.field, err)
		}
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
	created    []map[string]any
	exists     bool
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
		m.mu.Lock()
		exists := m.exists
		m.mu.Unlock()
		if exists {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"collectionName":"` + m.collection + `","numDocs":0,"collectionStatus":"ACTIVE","createdAt":1700000000,"updatedAt":1700000000,"dataUpdatedAt":1700000000}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"not found"}`))
	case r.Method == http.MethodPost && r.URL.Path == expectedBase:
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.created = append(m.created, body)
		m.exists = true
		m.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"collection":{"collectionName":"` + m.collection + `","numDocs":0,"collectionStatus":"ACTIVE","createdAt":1700000000,"updatedAt":1700000000,"dataUpdatedAt":1700000000}}`))
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
	case r.Method == http.MethodPost && r.URL.Path == path.Join(collectionPath, "docs", "fetch"):
		var body struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		idSet := map[string]bool{}
		for _, id := range body.IDs {
			idSet[id] = true
		}
		m.mu.Lock()
		docs := make([]map[string]any, 0, len(body.IDs))
		for _, doc := range m.accepted {
			id, _ := doc["id"].(string)
			if idSet[id] {
				docs = append(docs, map[string]any{
					"collection": m.collection,
					"doc":        doc,
				})
			}
		}
		m.mu.Unlock()
		response := map[string]any{
			"total":        len(docs),
			"took":         0,
			"docs":         docs,
			"isDocsInline": true,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			m.t.Errorf("encode fetch response: %v", err)
		}
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

func (m *lambdaDBMock) creates() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]map[string]any, len(m.created))
	copy(out, m.created)
	return out
}

func requireDocCount(t *testing.T, docs []map[string]any, want int) {
	t.Helper()
	if got := len(docs); got != want {
		t.Fatalf("mock accepted %d docs, want %d: %#v", got, want, docs)
	}
}

func requireDoc(t *testing.T, docs []map[string]any, id string) map[string]any {
	t.Helper()
	for _, doc := range docs {
		if doc["id"] == id {
			return doc
		}
	}
	t.Fatalf("docs = %#v, want ID %s", docs, id)
	return nil
}

func requireField(t *testing.T, doc map[string]any, field string) {
	t.Helper()
	if _, ok := doc[field]; !ok {
		t.Fatalf("doc = %#v, want field %q", doc, field)
	}
}

func requireCreatedIndexFields(t *testing.T, mock *lambdaDBMock, fields ...string) {
	t.Helper()
	creates := mock.creates()
	if len(creates) == 0 {
		t.Fatalf("LambdaDB mock did not record collection creation")
	}
	data, err := json.Marshal(creates)
	if err != nil {
		t.Fatalf("marshal collection creates: %v", err)
	}
	text := string(data)
	for _, field := range fields {
		if !strings.Contains(text, field) {
			t.Fatalf("collection create body = %s, want index field %q", text, field)
		}
	}
}
