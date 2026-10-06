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
		case "provider", "model", "sourceField", "dimensions", "similarity":
		default:
			unsupported = append(unsupported, "embedding."+key)
		}
	}
	if len(unsupported) > 0 {
		sort.Strings(unsupported)
		return components.IndexConfigsUnion{}, fmt.Errorf("unsupported embedding index options %v", unsupported)
	}
	return union, nil
}
