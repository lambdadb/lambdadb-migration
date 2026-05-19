package pinecone

import (
	"reflect"
	"testing"

	pineconeapi "github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestVectorToRecordConvertsValuesSparseAndMetadata(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{
		"title": "hello",
		"rank":  float64(3),
	})
	if err != nil {
		t.Fatalf("create metadata: %v", err)
	}
	values := []float32{0.1, 0.2}
	vector := &pineconeapi.Vector{
		Id:     "vec-1",
		Values: &values,
		SparseValues: &pineconeapi.SparseValues{
			Indices: []uint32{4, 8},
			Values:  []float32{0.7, 0.9},
		},
		Metadata: metadata,
	}

	record := vectorToRecord(vector)
	if record.ID != "vec-1" {
		t.Fatalf("record ID = %q, want vec-1", record.ID)
	}
	if record.Payload["title"] != "hello" || record.Payload["rank"] != float64(3) {
		t.Fatalf("payload = %#v, want metadata map", record.Payload)
	}
	if !reflect.DeepEqual(record.Vectors[""], source.VectorValue{Dense: []float32{0.1, 0.2}}) {
		t.Fatalf("dense vector = %#v", record.Vectors[""])
	}
	if !reflect.DeepEqual(record.Vectors["sparse"], source.VectorValue{Sparse: map[string]float32{"4": 0.7, "8": 0.9}}) {
		t.Fatalf("sparse vector = %#v", record.Vectors["sparse"])
	}
}

func TestCursorToToken(t *testing.T) {
	token, err := cursorToToken(source.Cursor{Value: "next-page"})
	if err != nil {
		t.Fatalf("cursorToToken() error = %v", err)
	}
	if token == nil || *token != "next-page" {
		t.Fatalf("token = %#v, want next-page", token)
	}

	token, err = cursorToToken(source.Cursor{})
	if err != nil {
		t.Fatalf("cursorToToken(nil) error = %v", err)
	}
	if token != nil {
		t.Fatalf("token = %#v, want nil", token)
	}
}

func TestSparseMapToValuesSortsIndices(t *testing.T) {
	got, err := sparseMapToValues(map[string]float32{"8": 0.9, "4": 0.7})
	if err != nil {
		t.Fatalf("sparseMapToValues() error = %v", err)
	}
	if !reflect.DeepEqual(got.Indices, []uint32{4, 8}) {
		t.Fatalf("indices = %#v, want sorted indices", got.Indices)
	}
	if !reflect.DeepEqual(got.Values, []float32{0.7, 0.9}) {
		t.Fatalf("values = %#v, want values sorted with indices", got.Values)
	}
}
