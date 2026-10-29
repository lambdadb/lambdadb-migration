package config

import (
	"fmt"
	"reflect"
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

func TestValidateMappingRejectsUnsupportedSourceSimilarity(t *testing.T) {
	for _, tt := range []struct {
		sourceKind, similarity, sourceSimilarity string
	}{
		{"qdrant", "dot_product", "Dot"},
		{"qdrant", "unsupported:manhattan", "Manhattan"},
		{"pinecone", "dot_product", "dotproduct"},
		{"elasticsearch", "dot_product", "dot_product"},
		{"elasticsearch", "max_inner_product", "max_inner_product"},
	} {
		t.Run(tt.sourceKind+"/"+tt.sourceSimilarity, func(t *testing.T) {
			inv := validationInventory()
			inv.SourceKind = tt.sourceKind
			inv.Vectors["title_dense"] = source.VectorField{Name: "title_dense", Dimensions: 768, Similarity: tt.similarity, SourceSimilarity: tt.sourceSimilarity}
			mapping := MappingFromInventory(inv, "articles")

			err := ValidateMapping(inv, mapping, "articles", WriteModeBulk)
			assertErrorContains(t, err, fmt.Sprintf(`vector "title_dense" uses unsupported similarity %q (source %s metric %q); LambdaDB supports cosine, euclidean.`, tt.similarity, tt.sourceKind, tt.sourceSimilarity))
		})
	}
}

// A mapping file can choose a supported similarity explicitly; the generated
// mapping never does so on its own.
func TestValidateMappingAcceptsExplicitSupportedSimilarity(t *testing.T) {
	inv := validationInventory()
	inv.Vectors["title_dense"] = source.VectorField{Name: "title_dense", Dimensions: 768, Similarity: "dot_product", SourceSimilarity: "Dot"}
	mapping := MappingFromInventory(inv, "articles")
	if got := mapping.Vectors["title_dense"].Similarity; got != "dot_product" {
		t.Fatalf("generated similarity = %q, want source similarity dot_product", got)
	}
	mapping.Vectors["title_dense"] = VectorMapping{TargetField: "title_dense", Dimensions: 768, Similarity: "cosine"}

	if err := ValidateMapping(inv, mapping, "articles", WriteModeBulk); err != nil {
		t.Fatalf("ValidateMapping() error = %v", err)
	}
}

func TestValidateMappingVectorDimensionBounds(t *testing.T) {
	for _, tt := range []struct {
		dimensions int64
		want       string
	}{
		{63, `vector "title_dense" dimensions 63 are below LambdaDB minimum 64`},
		{64, ""},
		{4096, ""},
		{4097, `vector "title_dense" dimensions 4097 exceed LambdaDB limit 4096`},
	} {
		t.Run(fmt.Sprint(tt.dimensions), func(t *testing.T) {
			inv := validationInventory()
			inv.Vectors["title_dense"] = source.VectorField{Name: "title_dense", Dimensions: tt.dimensions, Similarity: "cosine"}
			err := ValidateMapping(inv, MappingFromInventory(inv, "articles"), "articles", WriteModeBulk)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("ValidateMapping() error = %v", err)
				}
				return
			}
			assertErrorContains(t, err, tt.want)
		})
	}
}

func TestVectorFieldWarnings(t *testing.T) {
	for _, tt := range []struct {
		name   string
		vector source.VectorField
		want   []string
	}{
		{"supported", source.VectorField{Dimensions: 768, Similarity: "cosine", SourceSimilarity: "Cosine"}, nil},
		{"dot product", source.VectorField{Dimensions: 768, Similarity: "dot_product", SourceSimilarity: "Dot"}, []string{
			`vector "dense" uses Qdrant metric "Dot", which LambdaDB does not support (supported: cosine, euclidean); migration validation fails unless the mapping sets a supported similarity`,
		}},
		{"unreported dimensions", source.VectorField{Similarity: "cosine"}, nil},
		{"small dimensions and unknown metric", source.VectorField{Dimensions: 3, Similarity: "unknown"}, []string{
			`vector "dense" uses Qdrant metric "unknown", which LambdaDB does not support (supported: cosine, euclidean); migration validation fails unless the mapping sets a supported similarity`,
			`vector "dense" has 3 dimensions, outside the LambdaDB range 64-4096; migration validation fails`,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := VectorFieldWarnings("Qdrant", "dense", tt.vector); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("VectorFieldWarnings() = %q, want %q", got, tt.want)
			}
		})
	}
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
				Similarity: "euclidean",
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
