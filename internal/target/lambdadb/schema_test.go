package lambdadb

import (
	"testing"

	"github.com/lambdadb/go-lambdadb/models/components"
	"github.com/lambdadb/lambdadb-migration/internal/config"
)

func TestBuildIndexConfigs(t *testing.T) {
	cfgs, err := buildIndexConfigs(config.MappingConfig{
		Vectors: map[string]config.VectorMapping{
			"": {
				TargetField: "dense",
				Dimensions:  1536,
				Similarity:  "cosine",
			},
		},
		SparseVectors: map[string]config.SparseVectorMapping{
			"sparse": {TargetField: "sparse"},
		},
		Payload: config.PayloadMapping{
			IndexConfigs: map[string]map[string]any{
				"tenant_id": {"type": "keyword"},
				"title":     {"type": "text", "analyzers": []any{"english"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("buildIndexConfigs() error = %v", err)
	}

	if got := cfgs["dense"].Type; got != components.IndexConfigsUnionTypeVector {
		t.Fatalf("dense type = %v, want vector", got)
	}
	if got := cfgs["sparse"].Type; got != components.IndexConfigsUnionTypeSparseVector {
		t.Fatalf("sparse type = %v, want sparseVector", got)
	}
	if got := cfgs["tenant_id"].Type; got != components.IndexConfigsUnionTypeKeyword {
		t.Fatalf("tenant_id type = %v, want keyword", got)
	}
	if got := cfgs["title"].IndexConfigsText.Analyzers; len(got) != 1 || got[0] != components.AnalyzerEnglish {
		t.Fatalf("title analyzers = %v, want [english]", got)
	}
}

func TestBuildIndexConfigsRejectsManhattan(t *testing.T) {
	_, err := buildIndexConfigs(config.MappingConfig{
		Vectors: map[string]config.VectorMapping{
			"": {
				TargetField: "dense",
				Dimensions:  1536,
				Similarity:  "unsupported:manhattan",
			},
		},
	})
	if err == nil {
		t.Fatal("buildIndexConfigs() error = nil, want error")
	}
}
