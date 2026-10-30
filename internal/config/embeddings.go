package config

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/lambdadb/go-lambdadb/models/components"
)

// ParseEmbeddingIndexConfig preserves native omission and explicit legacy true.
// Use the SDK's decoder for structural validation; model support and source-field
// compatibility remain server validations. Never silently drop mapping options.
func ParseEmbeddingIndexConfig(index map[string]any) (components.IndexConfigsUnion, error) {
	var union components.IndexConfigsUnion
	raw, err := json.Marshal(index)
	if err != nil {
		return union, err
	}
	if err := json.Unmarshal(raw, &union); err != nil {
		return components.IndexConfigsUnion{}, err
	}
	if union.IndexConfigsNativeEmbeddingVector == nil && union.IndexConfigsManagedEmbeddingVector == nil {
		return components.IndexConfigsUnion{}, fmt.Errorf("payload vector index requires embedding; use vectors for stored source vectors")
	}

	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	var unsupported []string
	for key := range fields {
		switch key {
		case "type", "embedding", "managedEmbedding":
		default:
			unsupported = append(unsupported, key)
		}
	}
	var embedding map[string]json.RawMessage
	_ = json.Unmarshal(fields["embedding"], &embedding)
	for key := range embedding {
		switch key {
		case "provider", "model", "sourceField", "sources", "dimensions", "similarity":
		default:
			unsupported = append(unsupported, "embedding."+key)
		}
	}
	// The SDK decoder ignores unknown source keys, so check them here. Source
	// paths, types and the sourceField/sources choice remain server validations.
	var sources []map[string]json.RawMessage
	_ = json.Unmarshal(embedding["sources"], &sources)
	for i, source := range sources {
		for key := range source {
			switch key {
			case "field", "type":
			default:
				unsupported = append(unsupported, fmt.Sprintf("embedding.sources[%d].%s", i, key))
			}
		}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		return components.IndexConfigsUnion{}, fmt.Errorf("unsupported embedding index options %v", unsupported)
	}
	embeddingConfig := union.IndexConfigsNativeEmbeddingVector.GetEmbedding()
	if union.IndexConfigsManagedEmbeddingVector != nil {
		embeddingConfig = union.IndexConfigsManagedEmbeddingVector.GetEmbedding()
	}
	if similarity := embeddingConfig.GetSimilarity(); similarity != nil && !isSupportedSimilarity(string(*similarity)) {
		return components.IndexConfigsUnion{}, fmt.Errorf("unsupported embedding similarity %q; LambdaDB supports %s", *similarity, supportedSimilarities)
	}
	if dimensions := embeddingConfig.GetDimensions(); dimensions != nil && (*dimensions < MinLambdaDBVectorDimensions || *dimensions > MaxLambdaDBVectorDimensions) {
		return components.IndexConfigsUnion{}, fmt.Errorf("embedding dimensions %d are outside the LambdaDB range %d-%d", *dimensions, MinLambdaDBVectorDimensions, MaxLambdaDBVectorDimensions)
	}
	return union, nil
}
