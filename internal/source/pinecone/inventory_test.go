package pinecone

import (
	"reflect"
	"testing"

	pineconeapi "github.com/pinecone-io/go-pinecone/v5/pinecone"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestAddVectorConfigDense(t *testing.T) {
	dim := int32(1536)
	inv := emptyInventory()

	addVectorConfig(inv, &pineconeapi.Index{
		VectorType: "dense",
		Dimension:  &dim,
		Metric:     pineconeapi.Cosine,
	})

	want := source.VectorField{Name: "dense", Dimensions: 1536, Similarity: "cosine", SourceSimilarity: "cosine"}
	if vector := inv.Vectors[""]; vector != want {
		t.Fatalf("dense vector = %#v, want %#v", vector, want)
	}
	if len(inv.Warnings) != 0 {
		t.Fatalf("warnings = %q, want none", inv.Warnings)
	}
}

// Inventory keeps reporting a dotproduct index and warns that LambdaDB cannot
// create it; mapping validation then rejects the generated mapping.
func TestAddVectorConfigDenseDotproductWarns(t *testing.T) {
	dim := int32(1536)
	inv := emptyInventory()

	addVectorConfig(inv, &pineconeapi.Index{
		VectorType: "dense",
		Dimension:  &dim,
		Metric:     pineconeapi.Dotproduct,
	})

	want := source.VectorField{Name: "dense", Dimensions: 1536, Similarity: "dot_product", SourceSimilarity: "dotproduct"}
	if vector := inv.Vectors[""]; vector != want {
		t.Fatalf("dense vector = %#v, want %#v", vector, want)
	}
	wantWarnings := []string{`vector "dense" uses Pinecone metric "dotproduct", which LambdaDB does not support (supported: cosine, euclidean); migration validation fails unless the mapping sets a supported similarity`}
	if !reflect.DeepEqual(inv.Warnings, wantWarnings) {
		t.Fatalf("warnings = %q, want %q", inv.Warnings, wantWarnings)
	}
}

func TestAddVectorConfigSparse(t *testing.T) {
	inv := emptyInventory()

	addVectorConfig(inv, &pineconeapi.Index{VectorType: "sparse"})

	if _, ok := inv.SparseVectors["sparse"]; !ok {
		t.Fatalf("sparse vectors = %#v, want sparse field", inv.SparseVectors)
	}
	if len(inv.Vectors) != 0 {
		t.Fatalf("dense vectors = %#v, want none", inv.Vectors)
	}
}

func TestMapMetric(t *testing.T) {
	tests := map[pineconeapi.IndexMetric]string{
		pineconeapi.Cosine:     "cosine",
		pineconeapi.Euclidean:  "euclidean",
		pineconeapi.Dotproduct: "dot_product",
	}
	for metric, want := range tests {
		if got := mapMetric(metric); got != want {
			t.Fatalf("mapMetric(%q) = %q, want %q", metric, got, want)
		}
	}
}

func emptyInventory() *source.Inventory {
	return &source.Inventory{
		Vectors:        map[string]source.VectorField{},
		SparseVectors:  map[string]source.SparseVectorField{},
		PayloadIndexes: map[string]source.PayloadIndex{},
	}
}
