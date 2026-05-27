package elasticsearch

import (
	"context"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestInventoryMapsElasticsearchFields(t *testing.T) {
	server := newTestServer(t, map[string]handlerFunc{
		"GET /articles/_mapping": func(t *testing.T, w responseWriter, r request) {
			writeJSON(t, w, map[string]any{
				"articles": map[string]any{
					"mappings": map[string]any{
						"properties": map[string]any{
							"title": map[string]any{
								"type": "text",
								"fields": map[string]any{
									"keyword": map[string]any{"type": "keyword"},
								},
							},
							"url":          map[string]any{"type": "keyword"},
							"published_at": map[string]any{"type": "date"},
							"page_rank":    map[string]any{"type": "double"},
							"embedding": map[string]any{
								"type":       "dense_vector",
								"dims":       384,
								"similarity": "l2_norm",
							},
							"metadata": map[string]any{
								"properties": map[string]any{
									"source":   map[string]any{"type": "keyword"},
									"language": map[string]any{"type": "keyword"},
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
	})

	src, err := New(config.ElasticsearchConfig{URL: server.URL, Index: "articles"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	inv, err := src.Inventory(context.Background())
	if err != nil {
		t.Fatalf("Inventory() error = %v", err)
	}

	if got, want := inv.SourceKind, "elasticsearch"; got != want {
		t.Fatalf("SourceKind = %q, want %q", got, want)
	}
	if got, want := inv.RecordCount, uint64(2); got != want {
		t.Fatalf("RecordCount = %d, want %d", got, want)
	}
	assertVector(t, inv, "embedding", 384, "euclidean")
	assertPayloadIndex(t, inv, "title", "text")
	assertPayloadIndex(t, inv, "url", "keyword")
	assertPayloadIndex(t, inv, "published_at", "datetime")
	assertPayloadIndex(t, inv, "page_rank", "double")
	assertPayloadIndex(t, inv, "metadata.source", "keyword")
	assertPayloadIndex(t, inv, "metadata.language", "keyword")

	mapping := config.MappingFromInventory(inv, "articles")
	if got, want := mapping.Vectors["embedding"].TargetField, "embedding"; got != want {
		t.Fatalf("vector target = %q, want %q", got, want)
	}
	if got, want := mapping.Payload.Rename["metadata.source"], "metadata_source"; got != want {
		t.Fatalf("payload rename = %q, want %q", got, want)
	}
	if err := config.ValidateMapping(inv, mapping, "articles", config.WriteModeBulk); err != nil {
		t.Fatalf("ValidateMapping() error = %v", err)
	}
}

func assertVector(t *testing.T, inv *source.Inventory, name string, dimensions int64, similarity string) {
	t.Helper()
	vector, ok := inv.Vectors[name]
	if !ok {
		t.Fatalf("missing vector %q in %#v", name, inv.Vectors)
	}
	if vector.Dimensions != dimensions || vector.Similarity != similarity {
		t.Fatalf("vector %q = %#v, want dimensions=%d similarity=%q", name, vector, dimensions, similarity)
	}
}

func assertPayloadIndex(t *testing.T, inv *source.Inventory, name string, typ string) {
	t.Helper()
	index, ok := inv.PayloadIndexes[name]
	if !ok {
		t.Fatalf("missing payload index %q in %#v", name, inv.PayloadIndexes)
	}
	if index.Type != typ {
		t.Fatalf("payload index %q type = %q, want %q", name, index.Type, typ)
	}
}
