package transform

import (
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/config"
	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestRecordToDocumentPreservesIDByDefault(t *testing.T) {
	doc := RecordToDocument(source.Record{
		ID: "123",
		Payload: map[string]any{
			"title": "hello",
		},
	}, "")

	if got := doc["id"]; got != "123" {
		t.Fatalf("doc id = %v, want %q", got, "123")
	}
	if _, ok := doc["_source_id"]; ok {
		t.Fatal("unexpected _source_id without copy option")
	}
}

func TestRecordToDocumentCanCopyOriginalID(t *testing.T) {
	doc := RecordToDocument(source.Record{ID: "abc"}, "_source_id")

	if got := doc["id"]; got != "abc" {
		t.Fatalf("doc id = %v, want %q", got, "abc")
	}
	if got := doc["_source_id"]; got != "abc" {
		t.Fatalf("_source_id = %v, want %q", got, "abc")
	}
}

func TestRecordToDocumentDefaultsUnnamedVectorToDense(t *testing.T) {
	doc := RecordToDocument(source.Record{
		ID: "abc",
		Vectors: map[string]source.VectorValue{
			"": {Dense: []float32{0.1, 0.2}},
		},
	}, "")

	if _, ok := doc["dense"]; !ok {
		t.Fatalf("doc = %#v, want dense vector field", doc)
	}
}

func TestRecordToDocumentWithMapping(t *testing.T) {
	doc, err := RecordToDocumentWithMapping(source.Record{
		ID: "123",
		Payload: map[string]any{
			"metadata.url": "https://example.com",
		},
		Vectors: map[string]source.VectorValue{
			"":       {Dense: []float32{0.1, 0.2}},
			"sparse": {Sparse: map[string]float32{"1": 0.5}},
		},
	}, config.MappingConfig{
		Vectors: map[string]config.VectorMapping{
			"": {TargetField: "body_vector"},
		},
		SparseVectors: map[string]config.SparseVectorMapping{
			"sparse": {TargetField: "body_sparse"},
		},
		Payload: config.PayloadMapping{
			Rename: map[string]string{"metadata.url": "metadata_url"},
		},
		IDs: config.IDMapping{
			TargetField:    "id",
			CopyOriginalTo: "_source_id",
		},
	})
	if err != nil {
		t.Fatalf("RecordToDocumentWithMapping() error = %v", err)
	}

	if doc["id"] != "123" || doc["_source_id"] != "123" {
		t.Fatalf("id fields = %#v", doc)
	}
	if doc["metadata_url"] != "https://example.com" {
		t.Fatalf("renamed payload field missing: %#v", doc)
	}
	if _, ok := doc["body_vector"]; !ok {
		t.Fatalf("mapped dense vector missing: %#v", doc)
	}
	if _, ok := doc["body_sparse"]; !ok {
		t.Fatalf("mapped sparse vector missing: %#v", doc)
	}
}

func TestRecordToDocumentWithMappingNormalizesPayloadFieldNames(t *testing.T) {
	doc, err := RecordToDocumentWithMapping(source.Record{
		ID:      "123",
		Payload: map[string]any{"metadata.url": "https://example.com"},
	}, config.MappingConfig{})
	if err != nil {
		t.Fatalf("RecordToDocumentWithMapping() error = %v", err)
	}
	if doc["metadata_url"] != "https://example.com" {
		t.Fatalf("doc = %#v, want normalized metadata_url", doc)
	}
	if _, ok := doc["metadata.url"]; ok {
		t.Fatalf("doc = %#v, did not expect dotted field", doc)
	}
}

func TestRecordToDocumentWithMappingRejectsFieldNameCollision(t *testing.T) {
	_, err := RecordToDocumentWithMapping(source.Record{
		ID: "123",
		Payload: map[string]any{
			"metadata.url": "https://example.com",
			"metadata_url": "https://other.example.com",
		},
	}, config.MappingConfig{})
	if err == nil {
		t.Fatal("RecordToDocumentWithMapping() error = nil, want collision error")
	}
}
