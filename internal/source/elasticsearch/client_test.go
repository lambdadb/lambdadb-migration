package elasticsearch

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestReadUsesPITSearchAfterAndExtractsVectors(t *testing.T) {
	var searchCalls int
	server := newTestServer(t, map[string]handlerFunc{
		"GET /articles/_mapping": func(t *testing.T, w responseWriter, r request) {
			writeJSON(t, w, map[string]any{
				"articles": map[string]any{
					"mappings": map[string]any{
						"properties": map[string]any{
							"title":     map[string]any{"type": "text"},
							"embedding": map[string]any{"type": "dense_vector", "dims": 3, "similarity": "cosine"},
							"metadata": map[string]any{
								"properties": map[string]any{
									"source": map[string]any{"type": "keyword"},
								},
							},
						},
					},
				},
			})
		},
		"GET /articles/_count": func(t *testing.T, w responseWriter, r request) {
			writeJSON(t, w, map[string]any{"count": 2})
		},
		"POST /articles/_pit": func(t *testing.T, w responseWriter, r request) {
			if got, want := r.URL.Query().Get("keep_alive"), "5m"; got != want {
				t.Fatalf("keep_alive = %q, want %q", got, want)
			}
			writeJSON(t, w, map[string]any{"id": "pit-1"})
		},
		"POST /_search": func(t *testing.T, w responseWriter, r request) {
			searchCalls++
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode search body: %v", err)
			}
			fields, ok := body["fields"].([]any)
			if !ok || len(fields) != 1 || fields[0] != "embedding" {
				t.Fatalf("fields = %#v, want [embedding]", body["fields"])
			}
			switch searchCalls {
			case 1:
				if _, ok := body["search_after"]; ok {
					t.Fatalf("first search included search_after: %#v", body["search_after"])
				}
				writeJSON(t, w, map[string]any{
					"pit_id": "pit-2",
					"hits": map[string]any{
						"hits": []any{
							map[string]any{
								"_id":     "doc-1",
								"_source": map[string]any{"title": "One", "metadata": map[string]any{"source": "blog"}},
								"fields":  map[string]any{"embedding": []any{[]any{0.1, 0.2, 0.3}}},
								"sort":    []any{float64(11)},
							},
							map[string]any{
								"_id":     "doc-2",
								"_source": map[string]any{"title": "Two", "embedding": []any{0.4, 0.5, 0.6}},
								"sort":    []any{float64(12)},
							},
						},
					},
				})
			case 2:
				assertSearchAfter(t, body["search_after"], float64(12))
				writeJSON(t, w, map[string]any{
					"pit_id": "pit-2",
					"hits":   map[string]any{"hits": []any{}},
				})
			default:
				t.Fatalf("unexpected search call %d", searchCalls)
			}
		},
		"DELETE /_pit": func(t *testing.T, w responseWriter, r request) {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode close PIT body: %v", err)
			}
			if got, want := body["id"], "pit-2"; got != want {
				t.Fatalf("close PIT id = %v, want %q", got, want)
			}
			writeJSON(t, w, map[string]any{"succeeded": true, "num_freed": 1})
		},
	})

	src, err := New(config.ElasticsearchConfig{URL: server.URL, Index: "articles"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	first, err := src.Read(context.Background(), source.Cursor{}, 2)
	if err != nil {
		t.Fatalf("Read() first error = %v", err)
	}
	if first.Done {
		t.Fatalf("first batch Done = true, want false")
	}
	if len(first.Records) != 2 {
		t.Fatalf("first batch records = %d, want 2", len(first.Records))
	}
	if got, want := first.Records[0].Payload["metadata.source"], "blog"; got != want {
		t.Fatalf("flattened metadata source = %v, want %q", got, want)
	}
	if _, ok := first.Records[0].Payload["embedding"]; ok {
		t.Fatalf("vector field leaked into payload: %#v", first.Records[0].Payload)
	}
	assertDenseVector(t, first.Records[0], "embedding", []float32{0.1, 0.2, 0.3})
	assertDenseVector(t, first.Records[1], "embedding", []float32{0.4, 0.5, 0.6})

	second, err := src.Read(context.Background(), *first.NextCursor, 2)
	if err != nil {
		t.Fatalf("Read() second error = %v", err)
	}
	if !second.Done {
		t.Fatalf("second batch Done = false, want true")
	}
}

func assertSearchAfter(t *testing.T, value any, want float64) {
	t.Helper()
	values, ok := value.([]any)
	if !ok || len(values) != 1 || values[0] != want {
		t.Fatalf("search_after = %#v, want [%v]", value, want)
	}
}

func assertDenseVector(t *testing.T, record source.Record, field string, want []float32) {
	t.Helper()
	vector, ok := record.Vectors[field]
	if !ok {
		t.Fatalf("missing vector %q in %#v", field, record.Vectors)
	}
	if len(vector.Dense) != len(want) {
		t.Fatalf("vector %q len = %d, want %d", field, len(vector.Dense), len(want))
	}
	for i := range want {
		if vector.Dense[i] != want[i] {
			t.Fatalf("vector %q[%d] = %v, want %v", field, i, vector.Dense[i], want[i])
		}
	}
}
