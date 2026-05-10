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
