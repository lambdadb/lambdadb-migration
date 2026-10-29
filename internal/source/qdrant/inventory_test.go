package qdrant

import (
	"reflect"
	"testing"

	qdrantapi "github.com/qdrant/go-client/qdrant"

	"github.com/lambdadb/lambdadb-migration/internal/source"
)

func TestAddVectorReportsSourceDistance(t *testing.T) {
	unsupported := func(metric string) []string {
		return []string{`vector "body" uses Qdrant metric "` + metric + `", which LambdaDB does not support (supported: cosine, euclidean); migration validation fails unless the mapping sets a supported similarity`}
	}
	for _, tt := range []struct {
		distance     qdrantapi.Distance
		want         source.VectorField
		wantWarnings []string
	}{
		{qdrantapi.Distance_Cosine, source.VectorField{Name: "body", Dimensions: 768, Similarity: "cosine", SourceSimilarity: "Cosine"}, nil},
		{qdrantapi.Distance_Euclid, source.VectorField{Name: "body", Dimensions: 768, Similarity: "euclidean", SourceSimilarity: "Euclid"}, nil},
		{qdrantapi.Distance_Dot, source.VectorField{Name: "body", Dimensions: 768, Similarity: "dot_product", SourceSimilarity: "Dot"}, unsupported("Dot")},
		{qdrantapi.Distance_Manhattan, source.VectorField{Name: "body", Dimensions: 768, Similarity: "unsupported:manhattan", SourceSimilarity: "Manhattan"}, unsupported("Manhattan")},
	} {
		t.Run(tt.distance.String(), func(t *testing.T) {
			inv := &source.Inventory{Vectors: map[string]source.VectorField{}}
			addVector(inv, "body", &qdrantapi.VectorParams{Size: 768, Distance: tt.distance})

			if got := inv.Vectors["body"]; got != tt.want {
				t.Fatalf("vector = %#v, want %#v", got, tt.want)
			}
			if !reflect.DeepEqual(inv.Warnings, tt.wantWarnings) {
				t.Fatalf("warnings = %q, want %q", inv.Warnings, tt.wantWarnings)
			}
		})
	}
}

func TestAddVectorWarnsAboutDimensionsBelowLambdaDBMinimum(t *testing.T) {
	inv := &source.Inventory{Vectors: map[string]source.VectorField{}}
	addVector(inv, "", &qdrantapi.VectorParams{Size: 4, Distance: qdrantapi.Distance_Cosine})

	want := []string{`vector "dense" has 4 dimensions, outside the LambdaDB range 64-4096; migration validation fails`}
	if !reflect.DeepEqual(inv.Warnings, want) {
		t.Fatalf("warnings = %q, want %q", inv.Warnings, want)
	}
}
