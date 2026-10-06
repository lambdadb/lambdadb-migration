package lambdadb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/lambdadb/go-lambdadb/models/apierrors"
	"github.com/lambdadb/go-lambdadb/models/components"
	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
	"github.com/lambdadb/lambdadb-migration/internal/transform"
	"gopkg.in/yaml.v3"
)

const nativeIndexJSON = `{"embedding":{"model":"text-embedding-3-small","provider":"openai","sourceField":"body"},"type":"vector"}`
const legacyIndexJSON = `{"embedding":{"model":"text-embedding-3-small","provider":"openai","sourceField":"body"},"managedEmbedding":true,"type":"vector"}`
const normalizedIndexJSON = `{"embedding":{"dimensions":1536,"model":"text-embedding-3-small","provider":"openai","similarity":"cosine","sourceField":"body"},"managedEmbedding":true,"type":"vector"}`

func TestEmbeddingMappingToCollectionWire(t *testing.T) {
	for _, raw := range []string{
		nativeIndexJSON, legacyIndexJSON, normalizedIndexJSON,
		`{"embedding":{"dimensions":256,"model":"text-embedding-3-small","provider":"openai","sourceField":"body"},"type":"vector"}`,
		`{"embedding":{"dimensions":256,"model":"text-embedding-3-small","provider":"openai","similarity":"dot_product","sourceField":"body"},"type":"vector"}`,
	} {
		for _, format := range []string{"json", "yaml"} {
			t.Run(raw+"/"+format, func(t *testing.T) {
				inv := &source.Inventory{CollectionName: "source"}
				mapping := config.MappingFromInventory(inv, "articles")
				var index map[string]any
				if err := json.Unmarshal([]byte(raw), &index); err != nil {
					t.Fatal(err)
				}
				mapping.Payload.IndexConfigs["generated"] = index
				mapping.Payload.IndexConfigs["body"] = map[string]any{"type": "text"}
				var encoded []byte
				var err error
				if format == "json" {
					encoded, err = json.Marshal(mapping)
				} else {
					encoded, err = yaml.Marshal(mapping)
				}
				if err != nil {
					t.Fatal(err)
				}
				mapping, err = config.DecodeMapping(encoded)
				if err != nil {
					t.Fatal(err)
				}
				if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeUpsert); err != nil {
					t.Fatal(err)
				}
				posts := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Method == "GET" && r.URL.Path == collectionPath {
						w.WriteHeader(404)
						fmt.Fprint(w, `{"message":"not found"}`)
						return
					}
					if r.Method != "POST" || r.URL.Path != "/projects/test/collections" {
						t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
						w.WriteHeader(400)
						return
					}
					posts++
					var body struct {
						IndexConfigs map[string]json.RawMessage `json:"indexConfigs"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if got := string(body.IndexConfigs["generated"]); got != raw {
						t.Errorf("wire=%s want=%s", got, raw)
					}
					if got := string(body.IndexConfigs["body"]); got != `{"type":"text"}` {
						t.Errorf("text defaults changed: %s", got)
					}
					w.WriteHeader(201)
					fmt.Fprint(w, createdCollectionJSON)
				}))
				defer server.Close()
				if err := newContractTarget(server.URL, config.WriteModeUpsert).EnsureCollection(context.Background(), inv, mapping); err != nil {
					t.Fatal(err)
				}
				if posts != 1 {
					t.Fatalf("create calls=%d", posts)
				}
			})
		}
	}
}

func TestEmbeddingValidationAndSchemaErrors(t *testing.T) {
	for _, tc := range []struct{ name, patch, want string }{
		{"false", `{"managedEmbedding":false}`, "embedding is not allowed when managedEmbedding=false"},
		{"false before misplaced dimensions", `{"managedEmbedding":false,"dimensions":1536}`, "embedding is not allowed when managedEmbedding=false"},
		{"dimensions", `{"dimensions":1536}`, "Top-level dimensions"},
		{"similarity", `{"similarity":"cosine"}`, "Top-level similarity"},
		{"nonboolean", `{"managedEmbedding":"true"}`, "managedEmbedding"},
		{"missing embedding", `{"embedding":null,"managedEmbedding":true}`, "embedding is required"},
		{"stored vector", `{"embedding":null,"dimensions":3}`, "use vectors"},
		{"unknown option", `{"extra":true}`, "unsupported embedding index options [extra]"},
		{"unknown embedding option", `{"embedding":{"provider":"openai","model":"text-embedding-3-small","sourceField":"body","extra":true}}`, "unsupported embedding index options [embedding.extra]"},
		{"fractional dimensions", `{"embedding":{"provider":"openai","model":"text-embedding-3-small","sourceField":"body","dimensions":1.5}}`, "int64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var index map[string]any
			_ = json.Unmarshal([]byte(nativeIndexJSON), &index)
			if err := json.Unmarshal([]byte(tc.patch), &index); err != nil {
				t.Fatal(err)
			}
			inv := &source.Inventory{CollectionName: "source"}
			mapping := config.MappingFromInventory(inv, "articles")
			mapping.Payload.IndexConfigs["generated"] = index
			for _, check := range []func() error{
				func() error { return config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk) },
				func() error { _, err := buildIndexConfigs(mapping); return err },
			} {
				if err := check(); err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), `payload field "generated"`) {
					t.Fatalf("error=%v want=%q with field context", err, tc.want)
				}
			}
		})
	}
}

func TestEmbeddingCannotReplaceMappedSourceVector(t *testing.T) {
	inv := &source.Inventory{CollectionName: "source", Vectors: map[string]source.VectorField{"stored": {Name: "stored", Dimensions: 3, Similarity: "cosine"}}}
	mapping := config.MappingFromInventory(inv, "articles")
	var index map[string]any
	_ = json.Unmarshal([]byte(nativeIndexJSON), &index)
	mapping.Payload.IndexConfigs["stored"] = index
	if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk); err == nil || !strings.Contains(err.Error(), "field name collision") {
		t.Fatalf("error=%v", err)
	}
}

func TestNormalizedEmbeddingMetadataDoesNotUpdateCollection(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.Path != collectionPath {
			t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, strings.Replace(describedCollectionJSON, `"indexConfigs":{}`, `"indexConfigs":{"generated":`+normalizedIndexJSON+`}`, 1))
	}))
	defer server.Close()
	target := newContractTarget(server.URL, config.WriteModeUpsert)
	if err := target.EnsureCollection(context.Background(), nil, config.MappingConfig{Target: config.MappingTarget{CreateCollection: true}}); err != nil {
		t.Fatal(err)
	}
	meta, err := target.client.Collection("articles").Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	union := meta.IndexConfigs["generated"]
	if union.IndexConfigsManagedEmbeddingVector == nil {
		t.Fatal("normalized metadata must decode as legacy managed embedding")
	}
	raw, err := json.Marshal(union)
	if err != nil || string(raw) != normalizedIndexJSON {
		t.Fatalf("metadata=%s err=%v", raw, err)
	}
	legacy := components.CreateIndexConfigsUnionManagedEmbeddingVector(components.IndexConfigsManagedEmbeddingVector{Embedding: union.IndexConfigsManagedEmbeddingVector.Embedding})
	raw, err = json.Marshal(legacy)
	if err != nil || string(raw) != normalizedIndexJSON {
		t.Fatalf("legacy helper=%s err=%v", raw, err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestMigrationWritesPreserveVectorsAndText(t *testing.T) {
	inv := &source.Inventory{CollectionName: "source", Vectors: map[string]source.VectorField{"stored": {Name: "stored", Dimensions: 3, Similarity: "cosine"}}}
	mapping := config.MappingFromInventory(inv, "articles")
	// Native generation is explicitly requested for a separate destination field.
	var index map[string]any
	_ = json.Unmarshal([]byte(nativeIndexJSON), &index)
	mapping.Payload.IndexConfigs["generated"] = index
	mapping.Payload.Rename["source_body"] = "body"
	if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeUpsert); err != nil {
		t.Fatal(err)
	}
	doc, err := transform.RecordToDocumentWithMapping(source.Record{ID: "one", Payload: map[string]any{"source_body": "hello"}, Vectors: map[string]source.VectorValue{"stored": {Dense: []float32{0.25, -0.5, 1}}}}, mapping)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"docs":[{"body":"hello","id":"one","stored":[0.25,-0.5,1]}]}`
	// Verify transfer fidelity in both modes; this mock does not emulate server
	// schema validation. Native collections reject bulk completion (covered by
	// TestBulkWriteContract), while ordinary stored-vector collections accept it.
	for _, mode := range []config.WriteMode{config.WriteModeUpsert, config.WriteModeBulk} {
		t.Run(string(mode), func(t *testing.T) {
			writes, completions := 0, 0
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case mode == config.WriteModeBulk && r.Method == "GET" && r.URL.Path == collectionPath+"/docs/bulk-upsert":
					json.NewEncoder(w).Encode(map[string]any{"url": server.URL + "/upload", "objectKey": "object", "httpMethod": "PUT", "sizeLimitBytes": 200000000})
				case mode == config.WriteModeBulk && r.Method == "PUT" && r.URL.Path == "/upload",
					mode == config.WriteModeUpsert && r.Method == "POST" && r.URL.Path == collectionPath+"/docs/upsert":
					writes++
					raw, err := io.ReadAll(r.Body)
					if err != nil || string(raw) != want {
						t.Errorf("wire=%s want=%s err=%v", raw, want, err)
					}
					if mode == config.WriteModeUpsert {
						w.WriteHeader(http.StatusAccepted)
					}
					fmt.Fprint(w, `{}`)
				case mode == config.WriteModeBulk && r.Method == "POST" && r.URL.Path == collectionPath+"/docs/bulk-upsert":
					completions++
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{}`)
				default:
					t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			if err := newContractTarget(server.URL, mode).Write(context.Background(), []map[string]any{doc}); err != nil {
				t.Fatal(err)
			}
			if writes != 1 || (mode == config.WriteModeBulk && completions != 1) {
				t.Fatalf("writes=%d completions=%d", writes, completions)
			}
			raw, _ := json.Marshal(map[string]any{"docs": []map[string]any{doc}})
			if string(raw) != want {
				t.Fatalf("caller document mutated: %s", raw)
			}
		})
	}
	configs, err := buildIndexConfigs(config.MappingFromInventory(inv, "articles"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(configs["stored"])
	if string(raw) != `{"dimensions":3,"similarity":"cosine","type":"vector"}` {
		t.Fatalf("generated inventory changed stored vector contract: %s", raw)
	}
}

func TestEmbeddingServerErrorsRemainBadRequests(t *testing.T) {
	for _, operation := range []string{"create", "upsert", "knn"} {
		t.Run(operation, func(t *testing.T) {
			var methods []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				methods = append(methods, r.Method)
				w.Header().Set("Content-Type", "application/json")
				if operation == "create" && r.Method == "GET" {
					w.WriteHeader(404)
				} else {
					w.WriteHeader(400)
				}
				fmt.Fprint(w, `{"message":"native embedding input rejected"}`)
			}))
			defer server.Close()
			target := newContractTarget(server.URL, config.WriteModeUpsert)
			var err error
			wantMethods := []string{"POST"}
			switch operation {
			case "create":
				mapping := config.MappingFromInventory(&source.Inventory{CollectionName: "source"}, "articles")
				// Structurally valid input: the server still decides model compatibility.
				mapping.Payload.IndexConfigs["generated"] = map[string]any{"type": "vector", "embedding": map[string]any{"provider": "openai", "model": "unsupported", "sourceField": "body"}}
				err = target.EnsureCollection(context.Background(), nil, mapping)
				wantMethods = []string{"GET", "POST"}
			case "upsert":
				err = target.Write(context.Background(), []map[string]any{{"id": "one", "generated": []float32{1, 2, 3}}})
			case "knn":
				_, err = target.QueryKNN(context.Background(), "id", "generated", []float32{1, 2, 3}, 2)
				wantMethods = []string{"POST"}
			}
			var badRequest *apierrors.BadRequestError
			if !errors.As(err, &badRequest) {
				t.Fatalf("error=%T %v", err, err)
			}
			if !reflect.DeepEqual(methods, wantMethods) {
				t.Fatalf("calls=%v want=%v (no retries for invalid input)", methods, wantMethods)
			}
		})
	}
}
