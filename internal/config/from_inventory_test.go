package config

import (
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestMappingFromInventoryNormalizesFieldNames(t *testing.T) {
	mapping := MappingFromInventory(&source.Inventory{
		CollectionName: "articles",
		Vectors: map[string]source.VectorField{
			"title.vector": {
				Name:       "title.vector",
				Dimensions: 128,
				Similarity: "cosine",
			},
		},
		SparseVectors: map[string]source.SparseVectorField{
			"body.sparse": {Name: "body.sparse"},
		},
		PayloadIndexes: map[string]source.PayloadIndex{
			"metadata.url": {Name: "metadata.url", Type: "keyword"},
		},
	}, "articles")

	if got, want := mapping.Vectors["title.vector"].TargetField, "title_vector"; got != want {
		t.Fatalf("vector target = %q, want %q", got, want)
	}
	if got, want := mapping.SparseVectors["body.sparse"].TargetField, "body_sparse"; got != want {
		t.Fatalf("sparse vector target = %q, want %q", got, want)
	}
	if got, want := mapping.Payload.Rename["metadata.url"], "metadata_url"; got != want {
		t.Fatalf("payload rename = %q, want %q", got, want)
	}
	if _, ok := mapping.Payload.IndexConfigs["metadata_url"]; !ok {
		t.Fatalf("payload index configs = %#v, want metadata_url", mapping.Payload.IndexConfigs)
	}
}
