package qdrant

import (
	"testing"

	qdrantapi "github.com/qdrant/go-client/qdrant"
)

func TestPointIDToString(t *testing.T) {
	if got := pointIDToString(qdrantapi.NewIDNum(123)); got != "123" {
		t.Fatalf("numeric id = %q, want 123", got)
	}
	uuid := "550e8400-e29b-41d4-a716-446655440000"
	if got := pointIDToString(qdrantapi.NewIDUUID(uuid)); got != uuid {
		t.Fatalf("uuid id = %q, want %q", got, uuid)
	}
}

func TestSparseToMap(t *testing.T) {
	got := sparseToMap(&qdrantapi.SparseVector{
		Indices: []uint32{1, 42},
		Values:  []float32{0.2, 0.8},
	})
	if got["1"] != 0.2 || got["42"] != 0.8 {
		t.Fatalf("sparse map = %#v", got)
	}
}

func TestVectorOutputToValueSupportsDeprecatedDenseData(t *testing.T) {
	got := vectorOutputToValue(&qdrantapi.VectorOutput{
		Data: []float32{0.1, 0.2, 0.3},
	})
	if len(got.Dense) != 3 {
		t.Fatalf("dense vector = %#v, want deprecated data values", got.Dense)
	}
}

func TestVectorOutputToValueSupportsDeprecatedSparseData(t *testing.T) {
	got := vectorOutputToValue(&qdrantapi.VectorOutput{
		Data:    []float32{0.7, 0.2},
		Indices: &qdrantapi.SparseIndices{Data: []uint32{3, 9}},
	})
	if got.Dense != nil {
		t.Fatalf("dense vector = %#v, want sparse vector", got.Dense)
	}
	if got.Sparse["3"] != 0.7 || got.Sparse["9"] != 0.2 {
		t.Fatalf("sparse vector = %#v, want deprecated sparse data", got.Sparse)
	}
}

func TestVectorOutputToValueSupportsDeprecatedMultiData(t *testing.T) {
	vectorCount := uint32(2)
	got := vectorOutputToValue(&qdrantapi.VectorOutput{
		Data:         []float32{0.1, 0.2, 0.3, 0.4},
		VectorsCount: &vectorCount,
	})
	if got.Dense != nil {
		t.Fatalf("dense vector = %#v, want multi-vector", got.Dense)
	}
	if len(got.Multi) != 2 || len(got.Multi[0]) != 2 || got.Multi[1][1] != 0.4 {
		t.Fatalf("multi-vector = %#v, want deprecated multi-vector data", got.Multi)
	}
}
