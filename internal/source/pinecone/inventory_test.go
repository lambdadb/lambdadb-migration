package pinecone

import (
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
		Metric:     pineconeapi.Dotproduct,
	})

	vector := inv.Vectors[""]
	if vector.Name != "dense" || vector.Dimensions != 1536 || vector.Similarity != "dot_product" {
		t.Fatalf("dense vector = %#v", vector)
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
