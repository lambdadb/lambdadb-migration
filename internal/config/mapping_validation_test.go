package config

import (
	"strings"
	"testing"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestValidateMappingAcceptsGeneratedMapping(t *testing.T) {
	inv := validationInventory()
	mapping := MappingFromInventory(inv, "articles")

	if err := ValidateMapping(inv, mapping, "articles", WriteModeBulk); err != nil {
		t.Fatalf("ValidateMapping() error = %v", err)
	}
}

func TestValidateMappingRejectsTargetCollectionMismatch(t *testing.T) {
	inv := validationInventory()
	mapping := MappingFromInventory(inv, "other")

	err := ValidateMapping(inv, mapping, "articles", WriteModeBulk)
	assertErrorContains(t, err, `target.collection "other" does not match CLI target collection "articles"`)
}

func TestValidateMappingRejectsMissingVectorMapping(t *testing.T) {
	inv := validationInventory()
	mapping := MappingFromInventory(inv, "articles")
	delete(mapping.Vectors, "title_dense")

	err := ValidateMapping(inv, mapping, "articles", WriteModeBulk)
	assertErrorContains(t, err, `missing vector mapping for source vector "title_dense"`)
}

func TestValidateMappingRejectsInvalidVectorDetails(t *testing.T) {
	inv := validationInventory()
	mapping := MappingFromInventory(inv, "articles")
	mapping.Vectors["title_dense"] = VectorMapping{
		TargetField: "title_dense",
		Dimensions:  4097,
		Similarity:  "unsupported:manhattan",
	}

	err := ValidateMapping(inv, mapping, "articles", WriteModeBulk)
	assertErrorContains(t, err, `dimensions 4097 exceed LambdaDB limit 4096`)
	assertErrorContains(t, err, `uses unsupported similarity "unsupported:manhattan"`)
}

func TestValidateMappingRejectsInvalidPayloadIndex(t *testing.T) {
	inv := validationInventory()
	mapping := MappingFromInventory(inv, "articles")
	mapping.Payload.IndexConfigs["title"] = map[string]any{
		"type":      "text",
		"analyzers": []any{"english", "martian"},
	}
	mapping.Payload.IndexConfigs["metadata.url"] = map[string]any{"type": "keyword"}
	mapping.Payload.IndexConfigs["geo"] = map[string]any{"type": "geo"}

	err := ValidateMapping(inv, mapping, "articles", WriteModeBulk)
	assertErrorContains(t, err, `payload field "title" has unsupported analyzer "martian"`)
	assertErrorContains(t, err, `payload index field "metadata.url" contains '.'`)
	assertErrorContains(t, err, `payload field "geo" has unsupported index type "geo"`)
}

func TestValidateMappingRejectsFieldNameCollision(t *testing.T) {
	inv := validationInventory()
	inv.PayloadIndexes["metadata.url"] = source.PayloadIndex{Name: "metadata.url", Type: "keyword"}
	inv.PayloadIndexes["metadata_url"] = source.PayloadIndex{Name: "metadata_url", Type: "keyword"}
	mapping := MappingFromInventory(inv, "articles")

	err := ValidateMapping(inv, mapping, "articles", WriteModeBulk)
	assertErrorContains(t, err, `field name collision: payload field "metadata.url" and payload field "metadata_url" both target "metadata_url"`)
}

func validationInventory() *source.Inventory {
	return &source.Inventory{
		SourceKind:     "qdrant",
		CollectionName: "articles",
		Vectors: map[string]source.VectorField{
			"": {
				Name:       "dense",
				Dimensions: 1536,
				Similarity: "cosine",
			},
			"title_dense": {
				Name:       "title_dense",
				Dimensions: 768,
				Similarity: "dot_product",
			},
		},
		SparseVectors: map[string]source.SparseVectorField{
			"sparse": {Name: "sparse"},
		},
		PayloadIndexes: map[string]source.PayloadIndex{
			"tenant_id": {Name: "tenant_id", Type: "keyword"},
		},
	}
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want it to contain %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err.Error(), want)
	}
}
