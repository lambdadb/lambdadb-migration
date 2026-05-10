package lambdadb

import (
	"fmt"

	"github.com/lambdadb/go-lambdadb/models/components"
	"github.com/lambdadb/lambdadb-migration/internal/config"
)

func buildIndexConfigs(mapping config.MappingConfig) (map[string]components.IndexConfigsUnion, error) {
	out := make(map[string]components.IndexConfigsUnion)

	for _, vector := range mapping.Vectors {
		if vector.TargetField == "" {
			return nil, fmt.Errorf("vector target field is required")
		}
		similarity, err := mapSimilarity(vector.Similarity)
		if err != nil {
			return nil, fmt.Errorf("vector %q: %w", vector.TargetField, err)
		}
		out[vector.TargetField] = components.CreateIndexConfigsUnionVector(components.IndexConfigsVector{
			Dimensions: vector.Dimensions,
			Similarity: &similarity,
		})
	}

	for _, sparse := range mapping.SparseVectors {
		if sparse.TargetField == "" {
			return nil, fmt.Errorf("sparse vector target field is required")
		}
		out[sparse.TargetField] = components.CreateIndexConfigsUnionSparseVector(components.IndexConfigs{})
	}

	for field, index := range mapping.Payload.IndexConfigs {
		union, err := buildPayloadIndexConfig(index)
		if err != nil {
			return nil, fmt.Errorf("payload field %q: %w", field, err)
		}
		out[field] = union
	}

	return out, nil
}

func mapSimilarity(value string) (components.Similarity, error) {
	switch value {
	case "", "cosine":
		return components.SimilarityCosine, nil
	case "euclidean":
		return components.SimilarityEuclidean, nil
	case "dot_product":
		return components.SimilarityDotProduct, nil
	case "max_inner_product":
		return components.SimilarityMaxInnerProduct, nil
	case "unsupported:manhattan":
		return "", fmt.Errorf("Manhattan distance has no direct LambdaDB similarity equivalent")
	default:
		return "", fmt.Errorf("unsupported similarity %q", value)
	}
}

func buildPayloadIndexConfig(index map[string]any) (components.IndexConfigsUnion, error) {
	typ, _ := index["type"].(string)
	switch typ {
	case "keyword":
		return components.CreateIndexConfigsUnionKeyword(components.IndexConfigs{}), nil
	case "long":
		return components.CreateIndexConfigsUnionLong(components.IndexConfigs{}), nil
	case "double":
		return components.CreateIndexConfigsUnionDouble(components.IndexConfigs{}), nil
	case "datetime":
		return components.CreateIndexConfigsUnionDatetime(components.IndexConfigs{}), nil
	case "boolean":
		return components.CreateIndexConfigsUnionBoolean(components.IndexConfigs{}), nil
	case "sparseVector":
		return components.CreateIndexConfigsUnionSparseVector(components.IndexConfigs{}), nil
	case "text":
		return components.CreateIndexConfigsUnionText(components.IndexConfigsText{
			Analyzers: parseAnalyzers(index["analyzers"]),
		}), nil
	case "object":
		return components.CreateIndexConfigsUnionObject(components.IndexConfigsObject{
			ObjectIndexConfigs: map[string]any{},
		}), nil
	case "", "unknown":
		return components.IndexConfigsUnion{}, fmt.Errorf("missing or unknown index type")
	default:
		return components.IndexConfigsUnion{}, fmt.Errorf("unsupported index type %q", typ)
	}
}

func parseAnalyzers(value any) []components.Analyzer {
	if value == nil {
		return nil
	}
	var raw []string
	switch v := value.(type) {
	case []string:
		raw = v
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				raw = append(raw, s)
			}
		}
	default:
		return nil
	}

	analyzers := make([]components.Analyzer, 0, len(raw))
	for _, name := range raw {
		switch name {
		case "standard":
			analyzers = append(analyzers, components.AnalyzerStandard)
		case "english":
			analyzers = append(analyzers, components.AnalyzerEnglish)
		case "korean":
			analyzers = append(analyzers, components.AnalyzerKorean)
		case "japanese":
			analyzers = append(analyzers, components.AnalyzerJapanese)
		}
	}
	return analyzers
}
